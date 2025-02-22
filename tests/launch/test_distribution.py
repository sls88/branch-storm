from typing import Tuple

from src.operation import Operation as op, CallObject as obj
from src.branch import Branch as br
from src.type_containers import MandatoryArgTypeContainer as m, OptionalArgTypeContainer as opt


def return_1_2_3():
    return 1, 2, 3

def pass_one_arg(arg: int) -> int:
    return arg

def pass_three_args(arg1: int, arg2: int, arg3: int) -> Tuple[int, int, int]:
    return arg1, arg2, arg3

def test_distribution_start_func_and_last_func():
    actual_result = br("trusted_to_enriched")[
        op(obj(return_1_2_3)()).op_name("f1"),
        op(obj(pass_one_arg)(m[int])).distribute_input_data.op_name("f2"),
        op(obj(pass_one_arg)(m[int])).op_name("f3"),
        op(obj(pass_one_arg)(m[int])).stop_distribution.op_name("f4"),
        op(obj(pass_one_arg)(m[int])).distribute_input_data.op_name("f5"),
        op(obj(pass_one_arg)(m[int])).op_name("f6"),
        op(obj(pass_one_arg)(m[int])).stop_distribution.op_name("f7"),
        op(obj(pass_three_args)(m[int], m[int], m[int])).op_name("f8")
    ].run()

    assert actual_result == (1, 2, 3)


def test_distribution_start_func_middle_func_last_func():
    actual_result = br("trusted_to_enriched")[
        op(obj(return_1_2_3)()).op_name("f1"),
        op(obj(pass_one_arg)(m[int])).distribute_input_data.op_name("f2"),
        op(obj(pass_one_arg)(m[int])).op_name("f3"),
        op(obj(pass_one_arg)(m[int])).stop_distribution.op_name("f4"),
        op(obj(pass_three_args)(m[int], m[int], m[int])).op_name("f5"),
        op(obj(pass_three_args)(m[int], m[int], m[int])).op_name("f6"),
        op(obj(pass_one_arg)(m[int])).distribute_input_data.op_name("f7"),
        op(obj(pass_one_arg)(m[int])).op_name("f8"),
        op(obj(pass_one_arg)(m[int])).stop_distribution.op_name("f9"),
        op(obj(pass_three_args)(m[int], m[int], m[int])).op_name("f10")
    ].run()

    assert actual_result == (1, 2, 3)


def test_distribution_start_func_middle_func_last_func1():
    actual_result = br("trusted_to_enriched")[
        op(obj(return_1_2_3)()).assign("val.arg1", "val.arg2", "val.arg3").op_name("f1"),
        op(obj(pass_one_arg)(arg="val.arg1")).distribute_input_data.op_name("f2"),
        op(obj(pass_one_arg)(arg="val.arg2")).op_name("f3"),
        op(obj(pass_one_arg)(arg="val.arg3")).stop_distribution.op_name("f4"),
        op(obj(pass_three_args)(m[int], m[int], m[int])).op_name("f5"),
        op(obj(pass_three_args)(m[int], m[int], m[int])).op_name("f6"),
        op(obj(pass_one_arg)(arg="val.arg1")).distribute_input_data.op_name("f7"),
        op(obj(pass_one_arg)(arg="val.arg2")).op_name("f8"),
        op(obj(pass_one_arg)(arg="val.arg3")).stop_distribution.op_name("f9"),
        op(obj(pass_three_args)(m[int], m[int], m[int])).op_name("f10")
    ].run()

    assert actual_result == (1, 2, 3)


def test_distribution_start_func_and_end_branch():
    actual_result = br("trusted_to_enriched")[
        op(obj(return_1_2_3)()).op_name("f1"),
        op(obj(pass_one_arg)(m[int])).distribute_input_data.op_name("f2"),
        op(obj(pass_one_arg)(m[int])).op_name("f3"),
        op(obj(pass_one_arg)(m[int])).stop_distribution.op_name("f4"),
        op(obj(pass_one_arg)(m[int])).distribute_input_data.op_name("f5"),
        op(obj(pass_one_arg)(m[int])).op_name("f6"),
        op(obj(pass_one_arg)(m[int])).op_name("f7"),
    ].run()

    assert actual_result == (1, 2, 3)


def test_distribution_start_branch_and_last_func():
    actual_result = br("trusted_to_enriched")[
        op(obj(pass_one_arg)(m[int])).op_name("f1"),
        op(obj(pass_one_arg)(m[int])).op_name("f2"),
        op(obj(pass_one_arg)(m[int])).stop_distribution.op_name("f3"),
        op(obj(pass_one_arg)(m[int])).distribute_input_data.op_name("f4"),
        op(obj(pass_one_arg)(m[int])).op_name("f5"),
        op(obj(pass_one_arg)(m[int])).stop_distribution.op_name("f6"),
        op(obj(pass_three_args)(m[int], m[int], m[int])).op_name("f7")
    ].distribute_input_data.run((1, 2, 3))

    assert actual_result == (1, 2, 3)
