import re
from dataclasses import dataclass
from typing import Tuple

import pytest

from src.branch_storm.operation import Operation as op, CallObject as obj
from src.branch_storm.type_containers import MandatoryArgTypeContainer as m, OptionalArgTypeContainer as opt


class OneRunMethodWOArgs:
    def __init__(self):
        self.arg1 = 100

    def method(self) -> Tuple[int, int]:
        return self.arg1, 5


class OneRunMethodBound:
    def __init__(self, arg1: int):
        self.arg1 = arg1

    def method(self, arg2: int) -> Tuple[int, int]:
        return self.arg1, arg2


class OneRunMethodStatic:
    @staticmethod
    def method(arg1: int) -> int:
        return arg1


class ManyRunMethods:
    def __init__(self):
        pass

    @staticmethod
    def method1():
        return 1

    @staticmethod
    def method2():
        return 2

    def method3(self):
        return 3


class ManyRunMethodsArgInInit:
    def __init__(self, *args):
        self.args = args

    @staticmethod
    def static_method(*args) -> Tuple:
        return args

    def return_init_args(self, arg) -> Tuple:
        return *self.args, arg


@dataclass
class InstanceStorage:
    one_meth_inst_bound: OneRunMethodBound = OneRunMethodBound(1)
    one_meth_inst_static: OneRunMethodStatic = OneRunMethodStatic()
    many_meth_inst: ManyRunMethods = ManyRunMethods()


def test_process_one_op_class_one_run_method_wo_meth():
    operation = op(obj(OneRunMethodWOArgs)())
    result = operation.run((1,))
    actual_op_stack = operation._opts.op_name
    actual_result = (result[0].arg1, result[1])

    assert actual_result == (100, (1,))
    assert actual_op_stack == 'OneRunMethodWOArgs()'


def test_process_one_op_class_one_run_method_wo_args():
    operation = op(obj(OneRunMethodWOArgs)().method())
    actual_result = operation.run((1, 2))
    actual_op_stack = operation._opts.op_name

    assert actual_result == ((100, 5), (1, 2))
    assert actual_op_stack == 'OneRunMethodWOArgs().method()'


def test_process_one_op_class_one_run_method_bound():
    operation = op(obj(OneRunMethodBound)(m[int]).method(m[int]))
    actual_result = operation.run((1, 2))
    actual_op_stack = operation._opts.op_name

    assert actual_result == ((1, 2), None)
    assert actual_op_stack == "OneRunMethodBound().method()"


def test_process_one_op_class_one_run_method_bound_with_arg_in_init():
    operation = op(obj(OneRunMethodBound)(100).method(m[int])).op_name("some_operation")
    actual_result = operation.run((1, 2))
    actual_op_stack = operation._opts.op_name

    assert actual_result == ((100, 1), (2,))
    assert actual_op_stack == 'some_operation'


def test_process_one_op_class_one_run_method_bound_with_kwarg_in_init():
    actual_result = op(obj(OneRunMethodBound)(arg1=100).method(m[int])).run((1, 2))

    assert actual_result == ((100, 1), (2,))


def test_process_one_op_class_one_run_method_bound_with_kwarg_in_meth():
    actual_result = op(obj(OneRunMethodBound)(m[int]).method(arg2=200)).run((1, 2))

    assert actual_result == ((1, 200), (2,))


def test_process_one_op_class_one_run_method_bound_incorrect_type_neg():
    with pytest.raises(TypeError, match=re.escape(
            "Len: 1; Arg type map: {'arg1': (<class 'int'>, <class 'str'>)}")):
        op(obj(OneRunMethodBound)(m[str]).method(arg2=200)).run((1, 2))


def test_process_one_op_class_one_run_method_static():
    operation = op(obj(OneRunMethodStatic)().method(m[int]))
    actual_result = operation.run((1, 2))

    assert actual_result == (1, (2,))


def test_process_one_op_get_static_instance():
    operation = op(obj(OneRunMethodStatic)())
    result = operation.run((1, 2))
    actual_result = (isinstance(result[0], OneRunMethodStatic), result[1])

    assert actual_result == (True, (1, 2))


def test_process_one_op_get_bound_instance():
    operation = op(obj(OneRunMethodBound)(m[int]))
    result = operation.run((1, 2))
    actual_result = (result[0].arg1, result[1])

    assert actual_result == (1, (2,))


def test_process_one_op_many_run_methods_bound_wo_args():
    operation = op(obj(ManyRunMethods)().method3())
    actual_result = operation.run((1,))
    actual_op_stack = operation._opts.op_name

    assert actual_result == (3, (1,))
    assert actual_op_stack == 'ManyRunMethods().method3()'


def test_process_one_op_many_run_methods_static_wo_args():
    operation = op(obj(ManyRunMethods)().method2())
    actual_result = operation.run()

    assert actual_result == (2, None)


def test_process_one_op_many_run_methods_return_init():
    operation = op(obj(ManyRunMethods)())
    result = operation.run()
    actual_result = (isinstance(result[0], ManyRunMethods), result[1])
    actual_op_stack = operation._opts.op_name

    assert actual_result == (True, None)
    assert actual_op_stack == 'ManyRunMethods()'


def test_process_one_op_many_run_methods_use_instance_bound():
    operation = op(obj(ManyRunMethodsArgInInit(1, "2", True)
                   ).return_init_args(arg=m(2)[int]))
    init_data = (100, 200, 300)
    actual_result = operation.run(init_data)
    actual_op_stack = operation._opts.op_name

    assert actual_result == ((1, "2", True, 200), (100, 300))
    assert actual_op_stack == ('ManyRunMethodsArgInInit('
                               'instance).return_init_args()')


def test_process_one_op_many_run_methods_use_instance_bound_method_from_string():
    operation = op(obj("is.many_meth_inst").method3())
    init_data = (100,)
    actual_result = operation.rw_inst({"is": InstanceStorage()}).run(init_data)
    actual_op_stack = operation._opts.op_name

    assert actual_result == (3, (100,))
    assert actual_op_stack == 'ManyRunMethods(instance).method3()'


def test_process_one_op_many_run_methods_use_instance_static_method_with_args_from_string():
    operation = op(obj("is.one_meth_inst_static").method(opt[int]))
    init_data = (100,)
    actual_result = operation.rw_inst({"is": InstanceStorage()}).run(init_data)
    actual_op_stack = operation._opts.op_name

    assert actual_result == (100, None)
    assert actual_op_stack == 'OneRunMethodStatic(instance).method()'
