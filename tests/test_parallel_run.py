from copy import deepcopy
from dataclasses import dataclass
from typing import List, Tuple

import pytest

from src.default.parallelism import thread_pool, create_init_data_sequence, parallelize_table_branches
from src.default.stubs import get_all_args_return_default_value
from src.operation import Operation as op, CallObject as obj
from src.branch import Branch as br, Branch
from src.type_containers import MandatoryArgTypeContainer as m, OptionalArgTypeContainer as opt


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


def return_int_one() -> int: return 1
def get_int_arg_and_plus_one(arg: int) -> int: return arg + 1
def read(table_name, init_data=100): return init_data
def transform(arg): return arg + 1


def write(arg) -> None:
    global actual_result
    actual_result += [arg]
    return None


@dataclass
class JobArgs:
    job_name: str = "trusted_to_enriched"
    threads: str = "2"


actual_result = []

dim_tables = [
    "dim_pale",
    "dim_sale",
    "dim_kale"
]

def dim_branches(table_name: str) -> Branch:
    return br(table_name)[
        obj(read)(table_name=table_name, init_data=opt[int]),
        obj(transform)(m[int]),
        obj(write)(m[int])]


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
             obj(parallelize_table_branches)("trusted_to_enriched",
                 table_branches, threads=m("ja.threads"))
        ].rw_inst({"ja": ja})
    }

    exec_result = objects_for_processing.get(ja.job_name).run()

    global actual_result
    assert sorted(actual_result) == [101, 101, 101]
    assert exec_result is None
    actual_result = []


def test_process_few_branches_parallel_with_initial_data(get_table_branches):
    ja = JobArgs()
    ja.threads = "2"
    initial_data = (1, 2, 3)

    table_branches = get_table_branches
    objects_for_processing = {
        "api_to_json": [...],
        "json_to_parquet": [...],
        "trusted_to_enriched": br("trusted_to_enriched")[
             obj(parallelize_table_branches)("trusted_to_enriched",
                 table_branches, threads=m("ja.threads"), idata_for_each=(initial_data,))
        ].rw_inst({"ja": ja})
    }

    exec_result = objects_for_processing.get(ja.job_name).run()

    global actual_result
    assert sorted(actual_result) == [2, 3, 4]
    assert exec_result is None
    actual_result = []
