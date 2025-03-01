import re
from dataclasses import dataclass
from typing import Tuple

import pytest
from src.operation import Operation as op, CallObject as obj
from src.type_containers import MandatoryArgTypeContainer as m


@dataclass
class BBB:
    bbb_field: int = 0


@dataclass
class AAA:
    aaa_field: int = 15
    second_field: BBB = BBB()


class OneRunMethodBound:
    def __init__(self, arg1: AAA, arg2: int):
        self.arg1 = arg1
        self.arg2 = arg2

    def method(self, arg3: int, arg4: BBB) -> Tuple[AAA, int, int, BBB]:
        return self.arg1, self.arg2, arg3, arg4


def get_aaa_bbb_fields(arg1: int, arg2: int) -> Tuple[int, int]:
    return arg1, arg2


def get_aaa_bbb_rw_instances(arg1: AAA, arg2: BBB) -> Tuple[AAA, BBB]:
    return arg1, arg2


def test_process_one_object_get_aaa_bbb_fields_value_in_func_kwargs():
    operation = op(obj(get_aaa_bbb_fields)(m("aaa.aaa_field"), arg2=m("aaa.second_field.bbb_field")[int]))
    operation._set_branch_stack("stack")
    actual_result = operation.rw_inst({"aaa": AAA()}).run((1, 2))
    actual_op_stack = operation._operation_stack

    assert actual_result == ((15, 0), (1, 2))
    assert actual_op_stack == 'stack -> get_aaa_bbb_fields'


def test_process_one_object_get_aaa_bbb_instances_in_func_kwargs():
    aaa = AAA()
    operation = op(obj(get_aaa_bbb_rw_instances)("aaa", arg2=m("aaa.second_field")[BBB]))
    actual_result = operation.rw_inst({"aaa": aaa}).run(())

    assert actual_result == ((aaa, aaa.second_field), None)


def test_process_one_object_get_aaa_bbb_instances_in_func_kwargs_first_pos_expanded_from_kwargs():
    aaa = AAA()
    operation = op(obj(get_aaa_bbb_rw_instances)(arg1="aaa", arg2=m("aaa.second_field")))
    actual_result = operation.rw_inst({"aaa": aaa}).run(())

    assert actual_result == ((aaa, aaa.second_field), None)


def test_process_one_object_get_rw_kwargs_typehint_in_class():
    aaa = AAA()
    bbb = BBB()
    operation = op(obj(OneRunMethodBound)(
        "aa", arg2=m[int]).method(200, arg4="bb"))
    operation._set_branch_stack("stack")
    actual_result = operation.rw_inst({"aa": aaa, "bb": bbb}).run((100,))
    actual_op_stack = operation._operation_stack

    assert actual_result == ((aaa, 100, 200, bbb), None)
    assert actual_op_stack == 'stack -> OneRunMethodBound.method'


def get_and_pass_one_arg(arg: int) -> int:
    return arg


def test_process_one_object_get_rw_args_in_func():
    aaa = AAA()
    operation = op(obj(get_and_pass_one_arg)(m("aa.second_field.bbb_field")[int]))
    actual_result = operation.rw_inst({"aa": aaa}).run(())

    assert actual_result == (0, None)


class OneRunMethodBoundRW:
    def __init__(self, arg1: int):
        self.arg1 = arg1

    def method(self, arg2: int) -> Tuple[int, int]:
        return self.arg1, arg2


@dataclass
class Storage:
    f: OneRunMethodBoundRW = OneRunMethodBoundRW(1)


def test_process_one_object_get_rw_args_in_class():
    operation = op(obj("s.f").method(arg2=m("a.second_field.bbb_field")))
    operation._set_branch_stack("stack")
    actual_result = operation.rw_inst({"a": AAA(), "s": Storage()}).run(())
    actual_op_stack = operation._operation_stack

    assert actual_result == ((1, 0), None)
    assert actual_op_stack == 'stack -> OneRunMethodBoundRW(ext_instance).method'


def test_process_one_object_get_rw_args_in_class_no_such_class_neg():
    operation = op(obj("inc_class.f").method(arg2=m("a.second_field.bbb_field")[int]))
    operation._set_branch_stack("stack")
    with pytest.raises(
            TypeError,
            match=re.escape("Operation: stack -> External instance from string: \"inc_class.f\". "
                            "No such alias \"inc_class\" in rw_inst. Existing_aliases: ['val', 'var'].")):
        operation.run(())


def test_process_one_object_get_rw_args_in_class_incorrect_method_neg():
    operation = op(obj("s.incorrect").method(m("a.second_field.bbb_field")))
    operation._set_branch_stack("stack")
    with pytest.raises(
            AttributeError,
            match='Operation: stack -> External instance from string: "s.incorrect". '
                  'The RW class "Storage" does not have attribute "incorrect"'):
        operation.rw_inst({"a": AAA(), "s": Storage()}).run(())
