import re
from typing import Any, Union

import pytest

from src.branch_storm.constants import STOP_CONSTANT
from src.branch_storm.launch_operations.errors import EmptyDataError, IncorrectParameterError
from src.branch_storm.operation import Operation as op, CallObject as obj
from src.branch_storm.branch import Branch as br
from src.branch_storm.type_containers import MandatoryArgTypeContainer as m

counter = 0


def read() -> int: return 1
def transform(arg: int) -> int: return arg

def write(arg: int) -> None:
    global counter
    counter += arg
    return None


def read_return_none(): return None
def transform_return_none(arg: int): return None
def return_stop_constant(arg: int) -> Union[Any, str]:
    return STOP_CONSTANT


def test_all_operations_must_be_executed_false_complete():
    actual_result = br("trusted_to_enriched")[
        op(obj(read)()),
        op(obj(transform)(m[int])),
        op(obj(write)(m[int]))
    ].run()

    global counter
    assert actual_result is None
    assert counter == 1
    counter = 0


def test_all_operations_must_be_executed_false_read_return_none():
    actual_result = br("trusted_to_enriched")[
        op(obj(read_return_none)()),
        op(obj(transform)(m[int])),
        op(obj(write)(m[int]))
    ].run()

    global counter
    assert actual_result is None
    assert counter == 0
    counter = 0


def test_all_operations_must_be_executed_true_complete():
    actual_result = br("trusted_to_enriched")[
        op(obj(read)()),
        op(obj(transform)(m[int])),
        op(obj(write)(m[int]))
    ].all_operations_must_be_executed(True).run()

    global counter
    assert actual_result is None
    assert counter == 1
    counter = 0


def test_all_operations_must_be_executed_true_read_return_none_neg():
    with pytest.raises(
            EmptyDataError,
            match=re.escape(
                "Operation: trusted_to_enriched -> transform.\nThe data was")):
        br("trusted_to_enriched")[
            op(obj(read_return_none)()),
            op(obj(transform)(m[int])),
            op(obj(write)(m[int]))
        ].all_operations_must_be_executed(True).run()

    global counter
    assert counter == 0
    counter = 0


def test_all_operations_must_be_executed_true_in_branch_transform_return_none_neg():
    with pytest.raises(
            EmptyDataError,
            match=re.escape(
                "Operation: trusted_to_enriched -> br1 -> transform.\nThe data was")):
        br("trusted_to_enriched")[
            op(obj(read)()),
            br("br1")[
                op(obj(transform)(m[int])),
                op(obj(transform_return_none)(m[int])),
                op(obj(transform)(m[int]))
            ],
            op(obj(write)(m[int]))
        ].all_operations_must_be_executed(True).run()

    global counter
    assert counter == 0
    counter = 0


def test_all_operations_must_be_executed_true_with_stop_further_operations_neg():
    with pytest.raises(
            IncorrectParameterError,
            match=re.escape(
                "\nOperation: trusted_to_enriched -> br1 -> transform.\nThe previous ope")):
        br("trusted_to_enriched")[
            op(obj(read)()),
            br("br1")[
                op(obj(transform)(m[int])),
                op(obj(return_stop_constant)(m[int])),
                op(obj(transform)(m[int]))
            ],
            op(obj(write)(m[int]))
        ].all_operations_must_be_executed(True).run()

    global counter
    assert counter == 0
    counter = 0


def test_return_stop_further_operations_constant():
    actual_result = br("trusted_to_enriched")[
            op(obj(read)()),
            br("br1")[
                op(obj(transform)(m[int])),
                op(obj(return_stop_constant)(m[int])),
                op(obj(transform)(m[int]))
            ],
            op(obj(write)(m[int]))
        ].run()

    assert actual_result == STOP_CONSTANT
    global counter
    assert counter == 0
    counter = 0


def test_return_stop_further_operations_constant_several_branches_with_def_args():
    actual_result = br("trusted_to_enriched")[
            br("br1")[
                op(obj(read)()),
                op(obj(transform)(m[int])),
                op(obj(return_stop_constant)(m[int])),
                op(obj(transform)(m[int])),
                op(obj(write)(m[int]))
            ],
            br("br2")[
                op(obj(read)()),
                op(obj(transform)(m[int])),
                op(obj(write)(m[int]))
            ].def_args(),
        ].run()

    assert actual_result == STOP_CONSTANT
    global counter
    assert counter == 0
    counter = 0


def test_all_operations_must_be_executed_true_in_branch_false_pos():
    actual_result = br("trusted_to_enriched")[
        op(obj(read)()),
        op(obj(transform)(m[int])),
        br("br1")[
            br("br2")[
                op(obj(transform)(m[int])).op_name("show1"),
                br("br3")[
                    op(obj(transform)(m[int])),
                    op(obj(transform)(m[int])),
                    op(obj(transform)(m[int])).hide_init_inf_from_logs(False),
                ].hide_init_inf_from_logs(True),
                op(obj(transform)(m[int])).op_name("show2")
            ].hide_init_inf_from_logs(False),
            op(obj(transform)(m[int])),
            op(obj(transform_return_none)(m[int])).hide_init_inf_from_logs(False),
            op(obj(transform)(m[int]))
        ].all_operations_must_be_executed(False),
        op(obj(write)(m[int])).def_args(15)
    ].all_operations_must_be_executed(True).hide_init_inf_from_logs(True).run()

    assert actual_result is None
    global counter
    assert counter == 15
    counter = 0


def test_all_raise_err_if_empty_data_read_return_none_neg():
    with pytest.raises(
            EmptyDataError,
            match=re.escape(
                "Operation: trusted_to_enriched -> transform.\nNo data was received.")):
        br("trusted_to_enriched")[
            op(obj(read_return_none)()),
            op(obj(transform)(m[int])).raise_err_if_empty_data,
            op(obj(write)(m[int]))
        ].run()


def test_all_raise_err_if_empty_data_data_not_empty():
    actual_result = br("trusted_to_enriched")[
        op(obj(read)()),
        op(obj(transform)(m[int])).raise_err_if_empty_data,
        op(obj(write)(m[int]))
    ].run()

    global counter
    assert actual_result is None
    assert counter == 1
    counter = 0
