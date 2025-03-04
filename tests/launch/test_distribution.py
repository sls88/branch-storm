import re
from typing import Tuple

import pytest

from _src.launch_operations.errors import RemainingArgsFoundError
from _src.operation import Operation as op, CallObject as obj
from _src.branch import Branch as br
from _src.type_containers import MandatoryArgTypeContainer as m


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
        obj(return_1_2_3)(),
        op(obj(pass_one_arg)(m[int])).distribute_input_data,
        obj(pass_one_arg)(m[int]),
        op(obj(pass_one_arg)(m[int])).stop_distribution,
        obj(pass_three_args)(m[int], m[int], m[int]),
        obj(pass_three_args)(m[int], m[int], m[int]),
        op(obj(pass_one_arg)(m[int])).distribute_input_data,
        obj(pass_one_arg)(m[int]),
        op(obj(pass_one_arg)(m[int])).stop_distribution,
        obj(pass_three_args)(m[int], m[int], m[int])
    ].run()

    assert actual_result == (1, 2, 3)


def test_distribution_assign_values_option():
    actual_result = br("trusted_to_enriched")[
        op(obj(return_1_2_3)()).assign("val.arg1", "val.arg2", "val.arg3"),
        op(obj(pass_one_arg)(arg=m("val.arg1")[int])).distribute_input_data,
        obj(pass_one_arg)(arg=m("val.arg2")[int]),
        op(obj(pass_one_arg)(arg=m("val.arg3")[int])).stop_distribution,
        obj(pass_three_args)(m[int], m[int], m[int])
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
        obj(pass_one_arg)(m[int]),
        obj(pass_one_arg)(m[int]),
        op(obj(pass_one_arg)(m[int])).stop_distribution,
        op(obj(pass_one_arg)(m[int])).distribute_input_data,
        obj(pass_one_arg)(m[int]),
        op(obj(pass_one_arg)(m[int])).stop_distribution,
        obj(pass_three_args)(m[int], m[int], m[int])
    ].distribute_input_data.run((1, 2, 3))

    assert actual_result == (1, 2, 3)


def return1() -> int: return 1
def return2() -> int: return 2
def return3() -> int: return 3

def test_distribution_wo_args_in_branch():
    actual_result = br("trusted_to_enriched")[
        obj(return1)(),
        obj(return2)(),
        op(obj(return3)()).stop_distribution.op_name("f3"),
        op(obj(pass_one_arg)(m[int])).distribute_input_data,
        obj(pass_one_arg)(m[int]),
        op(obj(pass_one_arg)(m[int])).stop_distribution,
    ].distribute_input_data.run()

    assert actual_result == (1, 2, 3)


def test_distribution_wo_args_in_first_operation():
    actual_result = br("trusted_to_enriched")[
        op(obj(return1)()).distribute_input_data,
        obj(return2)(),
        obj(return3)()
    ].run()

    assert actual_result == (1, 2, 3)


def test_distribution_branch_to_branch():
    actual_result = br("trusted_to_enriched")[
        br("br1")[obj(return1)(), obj(return2)(), obj(return3)()],
        br("br2")[
            obj(pass_one_arg)(m[int]),
            obj(pass_one_arg)(m[int]),
            obj(pass_one_arg)(m[int])].distribute_input_data
    ].distribute_input_data.run()

    assert actual_result == (1, 2, 3)


def test_remaining_args_found_neg():
    with pytest.raises(
            RemainingArgsFoundError,
            match=re.escape(
                "Operation: trusted_to_enriched -> pass_one_arg.\n"
                "After executing the operation, data was detected that was not involved\n"
                "in the initialization/call. Len 2; Their types: [<class 'int'>, <class 'int'>]")):
        br("trusted_to_enriched")[
            obj(return_1_2_3)(),
            obj(pass_one_arg)(m[int])
        ].run()
