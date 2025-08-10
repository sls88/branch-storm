import pytest

from src.branch_storm.operation import CallObject as obj
from src.branch_storm.type_containers import MandatoryArgTypeContainer as m, OptionalArgTypeContainer as opt


def empty_init_func(): return 1
def pass_arg(arg): return arg
def pass_seq_func(*args: bool): return args

class A:
    def __init__(self, arg = None):
        pass

    def method(self, arg = None):
        pass


@pytest.mark.parametrize(
    ("c_obj", "expected_result"),
    [
        (obj(empty_init_func)(), False),
        (obj(pass_arg)(m[int]), True),
        (obj(pass_arg)(1), False),
        (obj(pass_arg)(m(1)), True),
        (obj(pass_arg)(m("ja")), False),
        (obj(pass_arg)(m(seq=True)[bool]), True),
        (obj(pass_arg)(opt[int]), False),
        (obj(pass_arg)(opt(1)), False),
        (obj(pass_arg)(opt("ja")), False),
        (obj(A)(opt("ja")).method(), False),
        (obj(A)(m(1)).method(), True),
        (obj(A)(m[int]).method(), True),
        (obj(A)().method(m[int]), True),
        (obj(A)(12).method(15), False),
        (obj(A)().method(m("ja")), False),
        (obj(A)(m(1)).method(opt[int]), True)
    ],
)
def test_try_to_find_slot_for_arg(c_obj: obj, expected_result: bool):
    actual_result = c_obj._try_to_find_slot_for_arg()

    assert actual_result == expected_result
