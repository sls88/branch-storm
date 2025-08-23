import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from src.branch_storm.launch_operations.capture_manager import register_ops, \
    begin_capture, end_capture

from src.branch_storm.operation import CallObject


def op_add(x: int) -> int:
    return x + 1

def op_echo(x):
    return x

class OpMul:
    def __call__(self, x: int) -> int:
        return x * 2


@pytest.fixture(autouse=True)
def _isolation_registry(monkeypatch):
    import src.branch_storm.launch_operations.capture_manager as cp
    cp._OP_REGISTRY.clear()
    yield
    cp._OP_REGISTRY.clear()


def test_begin_end_capture_patches_and_restores_module_globals():
    register_ops(op_add, OpMul)

    orig_add = op_add
    orig_mul = OpMul

    sess = begin_capture()
    try:
        assert globals()["op_add"] is not orig_add
        assert globals()["OpMul"] is not orig_mul

        assert op_add is globals()["op_add"]
        assert OpMul is globals()["OpMul"]

        res_f = op_add(10)
        res_c = OpMul()(3)
        assert isinstance(res_f, CallObject)
        assert isinstance(res_c, CallObject)
    finally:
        end_capture(sess)

    assert globals()["op_add"] is orig_add
    assert globals()["OpMul"] is orig_mul
    assert op_add is orig_add
    assert OpMul is orig_mul


def test_nested_capture_keeps_patch_until_outer_ends():
    register_ops(op_echo)

    orig = op_echo
    sess1 = begin_capture()
    try:
        w1 = globals()["op_echo"]
        assert w1 is not orig
        assert op_echo is w1

        sess2 = begin_capture()
        try:
            w2 = globals()["op_echo"]
            assert w2 is w1
            assert op_echo is w1
        finally:
            end_capture(sess2)

        assert globals()["op_echo"] is w1
        assert op_echo is w1
    finally:
        end_capture(sess1)

    assert globals()["op_echo"] is orig
    assert op_echo is orig


def test_only_registered_objects_are_patched():
    register_ops(OpMul)

    orig_add = op_add
    orig_mul = OpMul

    sess = begin_capture()
    try:
        assert globals()["OpMul"] is not orig_mul
        assert OpMul is globals()["OpMul"]

        assert globals()["op_add"] is orig_add
        assert op_add is orig_add

        assert op_add(5) == 6
    finally:
        end_capture(sess)

    assert globals()["OpMul"] is orig_mul
    assert OpMul is orig_mul
    assert globals()["op_add"] is orig_add
    assert op_add is orig_add


def test_do_not_restore_if_name_rebound_during_capture():
    register_ops(op_add)

    original = op_add
    sess = begin_capture()
    try:
        wrapper = globals()["op_add"]
        assert wrapper is not original

        def shadow(*a, **k):
            return "rebound"
        globals()["op_add"] = shadow
    finally:
        end_capture(sess)

    assert globals()["op_add"] is not original
    assert globals()["op_add"].__name__ == "shadow"

    globals()["op_add"] = original


def test_class_and_function_both_wrap_to_callobject_under_capture():
    register_ops(op_echo, OpMul)

    sess = begin_capture()
    try:
        r1 = op_echo("x")
        r2 = OpMul()(7)
        assert isinstance(r1, CallObject)
        assert isinstance(r2, CallObject)
    finally:
        end_capture(sess)

    assert op_echo("x") == "x"
    assert OpMul()(7) == 14


def op_inc(x: int) -> int:
    return x + 1

class Doubler:
    def __call__(self, x: int) -> int:
        return x * 2


@pytest.mark.parametrize("workers,capture_tasks,plain_tasks,loops", [
    (200, 500, 500, 5),
    (200, 1000, 1000, 5),
])
def test_concurrent_capture_isolation(workers, capture_tasks, plain_tasks, loops):
    register_ops(op_inc, Doubler)

    orig_inc = op_inc
    orig_cls = Doubler

    start_barrier = object()

    def capturing():
        while start_barrier is None:
            pass
        sess = begin_capture()
        try:
            for i in range(loops):
                a = op_inc(i)
                b = Doubler()(i)
                assert isinstance(a, CallObject)
                assert isinstance(b, CallObject)
                if i % 2 == 0:
                    time.sleep(0.0005)
        finally:
            end_capture(sess)

    def plain():
        while start_barrier is None:
            pass
        for i in range(loops):
            assert op_inc(i) == i + 1
            assert Doubler()(i) == i * 2
            if i % 2 == 1:
                time.sleep(0.0005)

    futs = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for _ in range(plain_tasks):
            futs.append(ex.submit(plain))
        for _ in range(capture_tasks):
            futs.append(ex.submit(capturing))

        globals()["start_barrier"] = None
        for f in as_completed(futs):
            f.result()

    assert op_inc is orig_inc
    assert Doubler is orig_cls
