from copy import deepcopy
from dataclasses import dataclass
from typing import List, Tuple, Optional

import pytest

from _src.default.parallelism import thread_pool, create_init_data_sequence, parallelize_without_result
from _src.default.stubs import get_all_args_return_default_value
from _src.operation import Operation as op, CallObject as obj
from _src.branch import Branch as br, Branch
from _src.type_containers import MandatoryArgTypeContainer as m, OptionalArgTypeContainer as opt


@pytest.mark.parametrize(
    ("len_obj", "idata_for_all", "idata_for_each", "expected_result"),
    [
        (3, [1, 2, 3], (((), (), ()),),
            [(1, 2, 3, ()), (1, 2, 3, ()), (1, 2, 3, ())]),
        (3, None, ((1, 2, 3),),
            [(1,), (2,), (3,)]),
        (3, 1, None,
            [(1,), (1,), (1,)]),
        (3, (1, 2, 3), ((1, 2, 3), (10, 20, 30)),
            [(1, 2, 3, 1, 10), (1, 2, 3, 2, 20), (1, 2, 3, 3, 30)]),
        (3, (1, 2, 3), None,
            [(1, 2, 3), (1, 2, 3), (1, 2, 3)]),
        (3, [1, 2, 3], None,
            [(1, 2, 3), (1, 2, 3), (1, 2, 3)]),
        (3, (), None,
            [(), (), ()]),
        (3, None, None,
            [(), (), ()]),
        (3, (), ((1, 2, 3),),
            [(1,), (2,), (3,)]),
    ],
)
def test_create_init_data_sequence(len_obj, idata_for_all, idata_for_each, expected_result):
    actual_result = create_init_data_sequence(len_obj, idata_for_all, idata_for_each)

    assert actual_result == expected_result


@dataclass
class TableNameStorage:
    name: Optional[str] = None


def return_int_one() -> int: return 1
def get_int_arg_and_plus_one(arg: int) -> int: return arg + 1

def read(table_name, tns: TableNameStorage, init_data=100) -> Tuple[int, TableNameStorage]:
    tns.name = table_name
    return init_data, tns

def transform(arg): return arg + 1
def get_three_return_sum(arg1: int, arg2: int, arg3: int) -> int: return sum([arg1, arg2, arg3])

def write(arg: int, table_name: str) -> None:
    global actual_result, table_name_result
    actual_result += [arg]
    table_name_result += [table_name]
    return None


@dataclass
class JobArgs:
    job_name: str = "trusted_to_enriched"
    threads: str = "2"


actual_result = []
table_name_result = []

dim_tables = [
    "dim_pale",
    "dim_sale",
    "dim_kale"
]


def dim_branches(table_name: str) -> Branch:
    return br(table_name)[
        obj(read)(table_name=table_name, tns=m("tns"), init_data=opt[int]),
        obj(transform)(m[int]),
        op(obj(transform)(m[int])).assign("val.int_storage"),
        br("transformation_branch")[
            obj(transform)(m("val.int_storage")[int]),
            obj(transform)(m("val.int_storage")[int]),
            obj(transform)(m("val.int_storage")[int])
        ].distribute_input_data,
        obj(get_three_return_sum)(m[int], m[int], m[int]),
        obj(write)(m[int], table_name=m("tns.name")[str])
    ].rw_inst({"tns": TableNameStorage()})


@pytest.fixture
def get_table_branches():
    return list(map(dim_branches, dim_tables))


def test_process_few_branches_parallel_without_initial_data(get_table_branches):
    ja = JobArgs()
    ja.threads = "max"

    table_branches = get_table_branches
    objects_for_processing = {
        "api_to_json": [...],
        "json_to_parquet": [...],
        "trusted_to_enriched": br("trusted_to_enriched")[
             obj(parallelize_without_result)("trusted_to_enriched",
                                             table_branches, threads=m("ja.threads"))
        ].rw_inst({"ja": ja})
    }

    exec_result = objects_for_processing.get(ja.job_name).run()

    global actual_result, table_name_result
    assert sorted(actual_result) == [309, 309, 309]
    assert sorted(table_name_result) == ['dim_kale', 'dim_pale', 'dim_sale']
    assert exec_result is None
    actual_result = []
    table_name_result = []


def test_process_few_branches_parallel_with_initial_data(get_table_branches):
    ja = JobArgs()
    ja.threads = "2"
    initial_data = (1, 2, 3)

    table_branches = get_table_branches
    objects_for_processing = {
        "api_to_json": [...],
        "json_to_parquet": [...],
        "trusted_to_enriched": br("trusted_to_enriched")[
             obj(parallelize_without_result)("trusted_to_enriched",
                                             table_branches, threads=m("ja.threads"), idata_for_each=(initial_data,))
        ].rw_inst({"ja": ja})
    }

    exec_result = objects_for_processing.get(ja.job_name).run()

    global actual_result, table_name_result
    assert sorted(actual_result) == [12, 15, 18]
    assert sorted(table_name_result) == ['dim_kale', 'dim_pale', 'dim_sale']
    assert exec_result is None
    actual_result = []
    table_name_result = []
