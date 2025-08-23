import sys
from dataclasses import dataclass, field
from typing import Tuple, Optional

import pytest

import tests
from src.branch_storm import RunConfigurations, parallelize_without_result, \
    Values
from src.branch_storm.operation import Operation as op, CallObject as obj
from src.branch_storm.branch import Branch as br
from src.branch_storm.type_containers import MandatoryArgTypeContainer as m, OptionalArgTypeContainer as opt
from src.branch_storm.launch_operations.capture_manager import register_ops, \
    is_registered, operation, typed_alias
from tests.launch.two_test_func_in_module import get_int_arg_and_plus_one
from tests.launch.test_run import return_int_one, deep_func_nested


def func_nested(arg: int) -> int: return arg + 1
def summ_result(arg: int, result: int) -> int: return arg + result

def read(table_name): return table_name
@operation
def transform(arg): return f"Table: {arg}"
def write(arg) -> None:
    tn = f"{arg} has been written."
    global written_tables
    written_tables.append(tn)

def get_three_return_sum(arg1: int, arg2: int, arg3: int
                         ) -> int: return sum([arg1, arg2, arg3])

written_tables = []

def calculate_deep_nested_branch(arg: int, run_conf: RunConfigurations) -> int:
    return br("d_nested_br")[
        deep_func_nested(m[int]),
        br("dd_nested_br")[
            op(deep_func_nested(m[int])).op_name("deep_deep_func_nested"),
            op(deep_func_nested(m[int])).op_name("deep_deep_func_nested2").assign("val.field"),
            return_int_one()
        ],
        op(deep_func_nested(m[int])).op_name("end_sep_func")
    ].rw_inst({"run_conf": run_conf}).run(arg)


class OneRunMethodBound:
    def __init__(self, arg1: int):
        self.arg1 = arg1

    def method(self, arg2: int) -> Tuple[int, int]:
        return self.arg1, arg2


@dataclass
class TableNameStorage:
    name: Optional[str] = None


actual_result = []
table_name_result = []

def read_par(
        table_name,
        tns: TableNameStorage,
        init_data=100) -> Tuple[int, TableNameStorage]:
    tns.name = table_name
    return init_data, tns

def transform_par(arg): return arg + 1

def write_par(arg: int, table_name: str) -> None:
    global actual_result, table_name_result
    actual_result += [arg]
    table_name_result += [table_name]
    return None


register_ops(
    return_int_one, "tests.launch.two_test_func_in_module",
    func_nested, summ_result, read,
    write, calculate_deep_nested_branch,
    OneRunMethodBound, parallelize_without_result,
    read_par, transform_par, write_par, get_three_return_sum,
    deep_func_nested,
    exclude=["deep_func*"])


def test_is_registered():
    assert not is_registered(
        tests.launch.two_test_func_in_module.deep_func_nested)


def test_run_one_operation():
    actual_result = br("enriched_job")[
        return_int_one()
    ].run()

    assert actual_result == 1


def test_run_nested_functions():
    actual_result = br("enriched_job")[
        return_int_one(),
        op(get_int_arg_and_plus_one(m[int])).op_name("custom_func2_name"),
        get_int_arg_and_plus_one(arg=m[int]),
        br("nested")[
            func_nested(opt[int]),
            br("deep_nested")[
                deep_func_nested(m[int]),
                deep_func_nested(m[int])
            ],
            func_nested(m[int])
        ],
        get_int_arg_and_plus_one(m[int]),
        br("nested2")[
            get_int_arg_and_plus_one(m[int])
        ]
    ].run()

    assert actual_result == 9


def test_process_two_branches():
    actual_result = br("trusted_to_enriched_job")[
        br("dim_term")[
            op(read(table_name="dim_term")).op_name("f1"),
            op(transform(m[str])).op_name("f2"),
            op(write(m[str])).op_name("f3"),
        ],
        br("dim_pale")[
            read("dim_pale"),
            transform(m[str]),
            write(m[str]),
        ],
    ].run()

    global written_tables
    assert written_tables == [
        "Table: dim_term has been written.",
        "Table: dim_pale has been written."]
    assert actual_result is None
    written_tables = []


def test_run_nested_functions_with_branch_break():
    actual_result = br("enriched_job")[
        return_int_one(),
        op(get_int_arg_and_plus_one(m[int])),
        br("nested_br")[
            func_nested(m[int]),
            calculate_deep_nested_branch(m[int], m("run_conf")),
            op(func_nested(m[int])).burn_rem_args.op_name("after_sep_func")
        ],
        get_int_arg_and_plus_one(m[int]),
        br("nested_br2")[
            get_int_arg_and_plus_one(m[int])
        ],
        summ_result(m[int], m("val.field"))
    ].run()

    assert actual_result == 11


def test_class_one_run_method_bound():
    actual_result = br("enriched_job")[
        OneRunMethodBound(100).method(m[int])
    ].run(1)
    assert actual_result == (100, 1)


dim_tables = [
    "dim_pale",
    "dim_sale",
    "dim_kale"
]



tns = typed_alias("tns", TableNameStorage)
val = typed_alias("val", Values)


def dim_branches(table_name: str) -> br:
    return br(table_name)[
        read_par(table_name=table_name, tns=m(tns), init_data=opt[int]),
        transform_par(m[int]),
        op(transform_par(m[int])).assign(val.int_storage),
        br("transformation_branch")[
            transform_par(m(val.int_storage)[int]),
            obj(transform_par)(m(val.int_storage)[int]),
            transform_par(m(val.int_storage)[int])
        ].distribute_input_data,
        get_three_return_sum(m[int], m[int], m[int]),
        write_par(m[int], table_name=m(tns.name)[str])
    ].rw_inst({"tns": TableNameStorage()})


@pytest.fixture
def get_table_branches():
    return list(map(dim_branches, dim_tables))


@dataclass
class JobArgs:
    job_name: str = "trusted_to_enriched"
    threads: str = "2"


def test_process_few_branches_parallel_with_initial_data(get_table_branches):
    ja = JobArgs()
    ja.threads = "max"
    initial_data = (1, 2, 3)

    table_branches = get_table_branches
    objects_for_processing = {
        "api_to_json": [...],
        "json_to_parquet": [...],
        "trusted_to_enriched": br("trusted_to_enriched")[
             obj(parallelize_without_result)(
                 m("run_conf"), table_branches, threads=m("ja.threads"),
                 idata_for_each=(initial_data,))
        ].rw_inst({"ja": ja})
    }

    exec_result = objects_for_processing.get(ja.job_name).run()

    global actual_result, table_name_result
    assert sorted(actual_result) == [12, 15, 18]
    assert sorted(table_name_result) == ['dim_kale', 'dim_pale', 'dim_sale']
    assert exec_result is None
    actual_result = []
    table_name_result = []


@dataclass
class BBB:
    bbb_field: int = 0


@dataclass
class AAA:
    aaa_field: int = 15
    second_field: BBB = field(default_factory=BBB)


def get_aaa_bbb_fields(arg1: int, arg2: int) -> Tuple[int, int]:
    return arg1, arg2



aaa = typed_alias("aaa", AAA)
register_ops(get_aaa_bbb_fields)


def test_attr_capture():
    actual_result = br("test")[
        get_aaa_bbb_fields(
            m(aaa.aaa_field),
            arg2=m(aaa.second_field.bbb_field)[int])
    ].rw_inst({"aaa": AAA()}).run()

    assert actual_result == (15, 0)
