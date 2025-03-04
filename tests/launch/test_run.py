from typing import Tuple

import pytest
from _src.operation import Operation as op, CallObject as obj
from _src.branch import Branch as br
from _src.type_containers import MandatoryArgTypeContainer as m, OptionalArgTypeContainer as opt


def return_int_one() -> int: return 1
def get_int_arg_and_plus_one(arg: int) -> int: return arg + 1
def func_nested(arg: int) -> int: return arg + 1
def deep_func_nested(arg: int) -> int: return arg + 1
def get_one_arg_return_two(arg: int) -> Tuple[int, int]:
    return arg + 1, 1
def return_none_inp_arg(arg: int) -> None: return None
def return_none_wo_arg() -> None: return None


def test_run_nested_functions():
    actual_result = br("enriched_job")[
        obj(return_int_one)(),
        op(obj(get_int_arg_and_plus_one)(m[int])).op_name("custom_func2_name"),
        obj(get_int_arg_and_plus_one)(arg=m[int]),
        br("nested")[
            obj(func_nested)(opt[int]),
            br("deep_nested")[
                obj(deep_func_nested)(m[int]),
                obj(deep_func_nested)(m[int])
            ],
            obj(func_nested)(m[int])
        ],
        obj(get_int_arg_and_plus_one)(m[int]),
        br("nested2")[
            obj(get_int_arg_and_plus_one)(m[int])
        ]
    ].run()

    assert actual_result == 9


def test_run_one_operation():
    actual_result = br("enriched_job")[
        obj(return_int_one)()
    ].run()

    assert actual_result == 1


def read(table_name): return table_name
def transform(arg): return f"Table: {arg}"
def write(arg) -> None:
    tn = f"{arg} has been written."
    global written_tables
    written_tables.append(tn)

written_tables = []


def test_process_two_branches_def_on_second_branch():
    actual_result = br("trusted_to_enriched_job")[
        br("dim_term")[
            op(obj(read)(table_name="dim_term")).op_name("f1"),
            op(obj(transform)(m[str])).op_name("f2"),
            op(obj(write)(m[str])).op_name("f3"),
        ],
        br("dim_pale")[
            obj(read)("dim_pale"),
            obj(transform)(m[str]),
            obj(write)(m[str]),
        ].def_args(),
    ].run()

    global written_tables
    assert written_tables == [
        "Table: dim_term has been written.",
        "Table: dim_pale has been written."]
    assert actual_result is None
    written_tables = []


def test_process_two_branches_error_field_in_def_rw_class():
    with pytest.raises(
            AttributeError,
            match="Operation: trusted_to_enriched_job -> dim_pale -> transform. "
                  "No such attribute in Variables"):
        br("trusted_to_enriched_job")[
            br("dim_term")[
                op(obj(read)(table_name="dim_term")).op_name("f1"),
                op(obj(transform)(m[str])).op_name("f2"),
                op(obj(write)(m[str])).op_name("f3"),
            ],
            br("dim_pale")[
                obj(read)("dim_pale"),
                obj(transform)(m("val.not_existed_field")),
                obj(write)(m[str]),
            ].def_args(),
        ].run()

    global written_tables
    assert written_tables == [
        "Table: dim_term has been written."]
    written_tables = []


def test_process_two_branches_double_writing_in_def_rw_class_positive():
    actual_result = br("trusted_to_enriched_job")[
            br("dim_term")[
                op(obj(read)(table_name="dim_term")),
                op(obj(transform)(m[str])).assign("val.field_result"),
                obj(return_none_wo_arg)()
            ],
            br("dim_pale")[
                obj(read)("dim_pale"),
                op(obj(transform)(m[str])).assign("val.field_result"),
                obj(return_none_wo_arg)()
            ].def_args(),
        ].run()

    assert actual_result is None


def test_process_two_branches_double_writing_in_def_rw_class_negative():
    with pytest.raises(
            ValueError,
            match="Operation: trusted_to_enriched_job -> dim_pale -> transform. "
                  "The value cannot be overwritten. The class is intended "
                  "for single-write and read use."):
        br("trusted_to_enriched_job")[
            br("dim_term")[
                op(obj(read)(table_name="dim_term")),
                op(obj(transform)(m[str])).assign("val.field_result"),
            ],
            br("dim_pale")[
                obj(read)("dim_pale"),
                op(obj(transform)(m[str])).assign("val.field_result"),
            ].def_args(),
        ].run()


def test_process_one_branch():
    actual_result = br("trusted_to_enriched_job")[
        br("dim_term")[
            obj(read)(table_name="dim_term"),
            obj(transform)(m[str]),
            obj(write)(m[str]),
        ],
        br("dim_pale")[
            obj(read)(table_name="dim_pale"),
            obj(transform)(m[str]),
            obj(write)(m[str]),
        ],
    ].run()

    global written_tables
    assert written_tables == [
        "Table: dim_term has been written."]
    assert actual_result is None
    written_tables = []
