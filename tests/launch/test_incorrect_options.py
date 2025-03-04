import re
from dataclasses import dataclass
from typing import Tuple

import pytest

from _src.launch_operations.errors import IncorrectParameterError, AssignmentError, DistributionError
from _src.operation import Operation as op, CallObject as obj
from _src.branch import Branch as br
from _src.type_containers import MandatoryArgTypeContainer as m, OptionalArgTypeContainer as opt


def return_1() -> int: return 1
def return_1_2() -> Tuple[int, int]: return 1, 2
def get_and_pass_arg_plus_1(arg: int) -> int: return arg + 1
def get_one_arg_return_two(arg: int) -> Tuple[int, int]:
    return arg + 1, 1
def get_arg_return_none(arg: int) -> None: return None
def return_none() -> None: return None


def test_incorrect_name_in_nested_branch_top():
    with pytest.raises(IncorrectParameterError, match=re.escape(
            "ion: enriched_job -> nested_br -> get_and_pass_arg_plus_1. The name")):
        br("enriched_job")[
            obj(return_1)(),
            obj(get_and_pass_arg_plus_1)(arg=m[int]),
            br("nested_br")[
                obj(get_and_pass_arg_plus_1)(m[int]),
                br("deep_nested_br")[
                    op(obj(get_and_pass_arg_plus_1)(m[int])).op_name(111),
                ],
            ],
            obj(get_and_pass_arg_plus_1)(m[int]),
        ].run()


def test_incorrect_name_in_nested_branch_level_less():
    with pytest.raises(IncorrectParameterError, match=re.escape(
            "ion: enriched_job -> nested_br -> deep_nested_br -> get_and_pass_arg_plus_1. The name")):
        br("enriched_job")[
            obj(return_1)(),
            obj(get_and_pass_arg_plus_1)(arg=m[int]),
            br("nested_br")[
                obj(get_and_pass_arg_plus_1)(m[int]),
                br("deep_nested_br")[
                    op(obj(get_and_pass_arg_plus_1)(m[int])),
                ],
            ],
            op(obj(get_and_pass_arg_plus_1)(m[int])).op_name(111),
        ].run()


def test_incorrect_name_initial_run():
    with pytest.raises(IncorrectParameterError, match=re.escape(
            "The last successful operation: INITIAL RUN. The name passed")):
        br("enriched_job")[
            op(obj(return_1)()).op_name(111),
        ].run()


def test_incorrect_name_single_run():
    with pytest.raises(IncorrectParameterError, match=re.escape(
            "The last successful operation: INITIAL RUN. The name passed")):
        op(obj(return_1)()).op_name(111).run()


@dataclass
class Storage:
    field: int = 0


def test_incorrect_assignment_rw_alias():
    with pytest.raises(AssignmentError, match=re.escape(
            'Operation: return_1_2. Alias "b" is missing from rw_inst. Assignment not possible.')):
        op(obj(return_1_2)()).assign("a.field", "b.field").rw_inst({"a": Storage()}).run()


def test_incorrect_assignment_fields_amount():
    with pytest.raises(AssignmentError, match=re.escape(
            'Operation: return_1_2. The number of positional arguments after the '
            'operation execution is 2 and it is not equal to the number of '
            'fields to assign, they were found 1')):
        op(obj(return_1_2)()).assign("s.field").rw_inst({"s": Storage()}).run()


def test_incorrect_assignment_bad_field_name():
    with pytest.raises(AssignmentError, match=re.escape(
            'Operation: return_1_2.\nPart of string reference '
            'to an object "1field" cannot be a python field.')):
        op(obj(return_1_2)()).assign("s.1field").rw_inst({"s": Storage()}).run()


def test_incorrect_assignment_result_none_for_assignment():
    with pytest.raises(AssignmentError, match=re.escape(
            'Operation: return_none. The result of the operation is None. Assignment is not possible.')):
        op(obj(return_none)()).assign("s.field").rw_inst({"s": Storage()}).run()


def test_incorrect_assignment_type_of_field_not_str():
    with pytest.raises(TypeError, match=re.escape(
            'Operation: return_1_2. All values to assign must be string only.')):
        op(obj(return_1_2)()).assign(111).rw_inst({"s": Storage()}).run()



def test_assignment_error_in_nested_branch_top():
    with pytest.raises(AssignmentError, match=re.escape(
            'Operation: enriched_job -> nested_br -> deep_nested_br -> get_and_pass_arg_plus_1.\n'
            'Part of string reference to an object "1field" cannot be a python field.')):
        br("enriched_job")[
            obj(return_1)(),
            obj(get_and_pass_arg_plus_1)(arg=m[int]),
            br("nested_br")[
                obj(get_and_pass_arg_plus_1)(m[int]),
                br("deep_nested_br")[
                    op(obj(get_and_pass_arg_plus_1)(m[int])).assign("var.1field"),
                ],
            ],
            op(obj(get_and_pass_arg_plus_1)(m[int])),
        ].run()


def test_assignment_error_in_nested_branch_level_less():
    with pytest.raises(AssignmentError, match=re.escape(
            'Operation: enriched_job -> get_and_pass_arg_plus_1.\n'
            'Part of string reference to an object "1field" cannot be a python field.')):
        br("enriched_job")[
            obj(return_1)(),
            obj(get_and_pass_arg_plus_1)(arg=m[int]),
            br("nested_br")[
                obj(get_and_pass_arg_plus_1)(m[int]),
                br("deep_nested_br")[
                    op(obj(get_and_pass_arg_plus_1)(m[int])),
                ],
            ],
            op(obj(get_and_pass_arg_plus_1)(m[int])).assign("var.1field"),
        ].run()


def test_incorrect_burn_rem_args_option():
    with pytest.raises(DistributionError, match=re.escape(
            'Operation: enriched_job -> return_1.\nIt is not possible to simultaneously')):
        br("enriched_job")[
            op(obj(return_1)()),
            op(obj(return_1)()).burn_rem_args
        ].distribute_input_data.run()


def test_incorrect_burn_rem_args_option_in_single_operation():
    with pytest.raises(DistributionError, match=re.escape(
            'Operation: return_1.\nIt is not possible to simultaneously')):
        op(obj(return_1)()).burn_rem_args.distribute_input_data.run()