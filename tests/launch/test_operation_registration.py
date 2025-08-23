import sys
import types
import pytest

from src.branch_storm.launch_operations.capture_manager import (
    register_ops, begin_capture, end_capture, operation)
from src.branch_storm.operation import CallObject


@pytest.fixture(autouse=True)
def _isolate_registry(monkeypatch):
    import src.branch_storm.launch_operations.capture_manager as cm
    cm._OP_REGISTRY.clear()
    yield
    cm._OP_REGISTRY.clear()


@pytest.fixture
def _cleanup_modules():
    added = []
    yield added
    for name in added:
        sys.modules.pop(name, None)


def _put_in_globals(name, obj):
    globals()[name] = obj
    return name

def _make_module(name: str) -> types.ModuleType:
    mod = types.ModuleType(name)
    mod.__dict__["__name__"] = name
    return mod

def _bind_obj_to_module(obj, modname: str, export_name: str):
    try:
        setattr(obj, "__module__", modname)
    except Exception:
        pass
    mod = sys.modules[modname]
    setattr(mod, export_name, obj)
    return obj


def test_operation_decorator_registers_func_and_class_and_capture_patches():
    @operation
    def dec_fn(x: int) -> int:
        return x + 1

    @operation
    class DecClass:
        def __call__(self, x: int) -> int:
            return x * 2

    _put_in_globals("dec_fn", dec_fn)
    _put_in_globals("DecClass", DecClass)

    sess = begin_capture()
    try:
        assert globals()["dec_fn"] is not dec_fn
        assert globals()["DecClass"] is not DecClass

        patched_fn = globals()["dec_fn"]
        patched_cls = globals()["DecClass"]

        assert isinstance(patched_fn(10), CallObject)
        assert isinstance(patched_cls()(3), CallObject)
    finally:
        end_capture(sess)

    assert globals()["dec_fn"] is dec_fn
    assert globals()["DecClass"] is DecClass


def test_register_ops_module_with_predicate_filters_objects(_cleanup_modules):
    import sys, inspect

    modname = "bs_tmp_mod_pred"
    mod = _make_module(modname)
    sys.modules[modname] = mod
    _cleanup_modules.append(modname)

    def op_add(x): return x + 1
    def op_mul(x): return x * 2
    def helper(x): return x

    class OpClass:
        def __call__(self, x): return x

    _bind_obj_to_module(op_add, modname, "op_add")
    _bind_obj_to_module(op_mul, modname, "op_mul")
    _bind_obj_to_module(helper, modname, "helper")
    _bind_obj_to_module(OpClass, modname, "OpClass")

    def only_add(name, obj):
        return inspect.isfunction(obj) and name.endswith("add")

    register_ops(sys.modules[modname], predicate=only_add)

    _put_in_globals("op_add_from_mod", mod.op_add)
    _put_in_globals("op_mul_from_mod", mod.op_mul)
    _put_in_globals("helper_from_mod", mod.helper)
    _put_in_globals("OpClass_from_mod", mod.OpClass)

    sess = begin_capture()
    try:
        assert globals()["op_add_from_mod"] is not mod.op_add
        assert globals()["op_mul_from_mod"] is mod.op_mul
        assert globals()["helper_from_mod"] is mod.helper
        assert globals()["OpClass_from_mod"] is mod.OpClass

        co = globals()["op_add_from_mod"]()
        assert isinstance(co, CallObject)

        assert globals()["op_mul_from_mod"](5) == 10
        assert globals()["helper_from_mod"](9) == 9

        inst = globals()["OpClass_from_mod"]()
        assert isinstance(inst, OpClass)
        assert inst(3) == 3
    finally:
        end_capture(sess)

    assert globals()["op_add_from_mod"] is mod.op_add
    assert globals()["op_mul_from_mod"] is mod.op_mul
    assert globals()["helper_from_mod"] is mod.helper
    assert globals()["OpClass_from_mod"] is mod.OpClass


def test_only_defined_in_owner_excludes_reexports(_cleanup_modules):
    src_name = "bs_src_mod"
    src = _make_module(src_name)
    sys.modules[src_name] = src
    _cleanup_modules.append(src_name)

    def real_op(x): return x + 7
    _bind_obj_to_module(real_op, src_name, "real_op")

    owner_name = "bs_owner_mod"
    owner = _make_module(owner_name)
    sys.modules[owner_name] = owner
    _cleanup_modules.append(owner_name)

    owner.real_op = src.real_op

    register_ops(sys.modules[owner_name], only_defined_in_owner=True)

    _put_in_globals("reexp", owner.real_op)

    sess = begin_capture()
    try:
        assert globals()["reexp"] is owner.real_op
        assert reexp(1) == 8
    finally:
        end_capture(sess)


def test_module_scan_can_register_callable_instance(_cleanup_modules):
    modname = "bs_mod_callable_inst"
    mod = _make_module(modname)
    sys.modules[modname] = mod
    _cleanup_modules.append(modname)

    class CallableObj:
        def __call__(self, x): return x + 100

    inst = CallableObj()
    _bind_obj_to_module(inst, modname, "callable_instance")

    register_ops(
        sys.modules[modname],
        include_callables_with_dunder_call=True,
    )

    _put_in_globals("callable_instance", mod.callable_instance)

    sess = begin_capture()
    try:
        assert globals()["callable_instance"] is not mod.callable_instance
        co = globals()["callable_instance"]()
        assert isinstance(co, CallObject)
    finally:
        end_capture(sess)
