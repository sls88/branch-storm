from typing import Tuple

from src.operation import Operation as op, CallObject as obj
from src.branch import Branch as br
from src.processor.initialization import GetPosArg
from src.type_containers import MandatoryArgTypeContainer as mand, OptionalArgTypeContainer as opt


def return_int_one() -> int: return 1
def get_int_arg_and_plus_one(arg: int) -> int: return arg + 1
def func_nested(arg: int) -> int: return arg + 1
def deep_func_nested(arg: int) -> int: return arg + 1
def get_one_arg_return_two(arg: int) -> Tuple[int, int]:
    return arg + 1, 1
def return_none(arg: int) -> None: return None


def test_run_nested_functions():
    actual_result = br("enriched_job")[
        op(obj(return_int_one)()),
        op(obj(get_int_arg_and_plus_one)(mand[int])).op_name("custom_func2_name"),
        op(obj(get_int_arg_and_plus_one)(arg=GetPosArg(mand[int], 1))),
        br("nested")[
            op(obj(func_nested)(opt[int])),
            br("deep_nested")[
                op(obj(get_one_arg_return_two)(mand[int])),
                op(obj(deep_func_nested)(mand[int]))
            ],
            op(obj(func_nested)(mand[int]))
        ],
        op(obj(get_int_arg_and_plus_one)(mand[int])),
        br("nested2")[
            op(obj(get_int_arg_and_plus_one)(mand[int]))
        ]
    ].run()

    assert actual_result == 9


def test_run_one_operation():
    actual_result = br("enriched_job")[
        op(obj(return_int_one)())
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
            op(obj(transform)(mand[str])).op_name("f2"),
            op(obj(write)(mand[str])).op_name("f3"),
        ],
        br("dim_pale")[
            op(obj(read)("dim_pale")),
            op(obj(transform)(mand[str])),
            op(obj(write)(mand[str])),
        ].def_args(),
    ].run()

    global written_tables
    assert written_tables == [
        "Table: dim_term has been written.",
        "Table: dim_pale has been written."]
    assert actual_result is None
    written_tables = []


def test_process_one_branch():
    actual_result = br("trusted_to_enriched_job")[
        br("dim_term")[
            op(obj(read)(table_name="dim_term")).op_name("f1"),
            op(obj(transform)(mand[str])).op_name("f2"),
            op(obj(write)(mand[str])).op_name("f3"),
        ],
        br("dim_pale")[
            op(obj(read)(table_name="dim_pale")),
            op(obj(transform)(mand[str])),
            op(obj(write)(mand[str])),
        ],
    ].run()

    global written_tables
    assert written_tables == [
        "Table: dim_term has been written."]
    assert actual_result is None
    written_tables = []
