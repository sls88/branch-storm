from copy import deepcopy

from src.launch_operations.launcher import run


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
    "dim_role",
    "dim_eea",
    "dim_third"
]


def dim_operations(table_name: str) -> add:
    return add(
        {read: {"table_name": table_name}},
        transform,
        write).op_name(table_name)


table_operations = list(map(dim_operations, dim_tables))


def test_process_few_branches_parallel_with_idata_for_each():
    ja = JobArgs()

    job_operations = [
        add({create_init_data_sequence: {
            "len_obj": len(table_operations),
            "idata_for_all": [deepcopy(ja), Values(), Variables()],
            "idata_for_each": ([1, 2, 3],)}}).delay_return,
        {create_arg_sequences: {
            "objects": table_operations,
            "rw_inst": {"ja": deepcopy(ja), "val": Values(), "var": Variables()},
            "op_name": "ja.job_name"}},
        create_kwargs,
        {thread_pool: {"threads": "ja.threads"}},
        {get_all_args_return_default_value: {"def_value": None}}
    ]

    objects_for_processing = {
        "api_to_json": [...],
        "json_to_parquet": [...],
        "trusted_to_enriched": job_operations
    }

    objects_for_curr_job = objects_for_processing.get(ja.job_name)

    exec_result = run(objects_for_curr_job,
                      # init_data=ja,
                      rw_inst={"ja": deepcopy(ja)},
                      distribute_args=(ja, ja),
                      op_name="trusted_to_enriched")
    global actual_result
    assert sorted(actual_result) == [2, 3, 4]
    assert exec_result is None
    actual_result = []


def test_process_few_branches_parallel_without_idata_for_each():
    ja = JobArgs()

    job_operations = [
        add({create_init_data_sequence: {
            "len_obj": len(table_operations),
            "idata_for_all": [deepcopy(ja), Values(), Variables()]}}).delay_return(True),
        {create_arg_sequences: {
            "objects": table_operations,
            "rw_inst": {"ja": deepcopy(ja), "val": Values(), "var": Variables()},
            "op_name": "ja.job_name"}},
        create_kwargs,
        {thread_pool: {"threads": "ja.threads"}},
        {get_all_args_return_default_value: {"def_value": None}}
    ]

    objects_for_processing = {
        "api_to_json": [...],
        "json_to_parquet": [...],
        "trusted_to_enriched": job_operations
    }

    objects_for_curr_job = objects_for_processing.get(ja.job_name)

    exec_result = run(objects_for_curr_job,
                      init_data=ja,
                      rw_inst={"ja": deepcopy(ja)},
                      op_name="trusted_to_enriched")
    global actual_result
    assert sorted(actual_result) == [101, 101, 101]
    assert exec_result is None
    actual_result = []


def test_process_few_branches_parallel_with_idata_for_eac_as_one_element():
    ja = JobArgs()

    job_operations = [
        add({create_init_data_sequence: {
            "len_obj": len(table_operations),
            "idata_for_all": 5}}).delay_return(True),
        {create_arg_sequences: {
            "objects": table_operations,
            "rw_inst": {"ja": deepcopy(ja), "val": Values(), "var": Variables()},
            "op_name": "ja.job_name"}},
        create_kwargs,
        {thread_pool: {"threads": "ja.threads"}},
        {get_all_args_return_default_value: {"def_value": None}}
    ]

    objects_for_processing = {
        "api_to_json": [...],
        "json_to_parquet": [...],
        "trusted_to_enriched": job_operations
    }

    objects_for_curr_job = objects_for_processing.get(ja.job_name)

    exec_result = run(objects_for_curr_job,
                      init_data=ja,
                      rw_inst={"ja": deepcopy(ja)},
                      op_name="trusted_to_enriched")
    global actual_result
    assert sorted(actual_result) == [6, 6, 6]
    assert exec_result is None
    actual_result = []


def test_process_few_branches_parallel_without_initial_data():
    ja = JobArgs()
    ja.threads = "max"

    job_operations = [
        add({create_init_data_sequence: {
            "len_obj": len(table_operations)}}).delay_return(True),
        {create_arg_sequences: {
            "objects": table_operations,
            "rw_inst": {"ja": deepcopy(ja), "val": Values(), "var": Variables()},
            "op_name": "ja.job_name",
            "def_args": ()}},
        create_kwargs,
        {thread_pool: {"threads": "ja.threads"}},
        {get_all_args_return_default_value: {"def_value": None}}
    ]

    objects_for_processing = {
        "api_to_json": [...],
        "json_to_parquet": [...],
        "trusted_to_enriched": job_operations
    }

    objects_for_curr_job = objects_for_processing.get(ja.job_name)

    exec_result = run(objects_for_curr_job,
                      init_data=ja,
                      rw_inst={"ja": deepcopy(ja)},
                      op_name="trusted_to_enriched")
    global actual_result
    assert sorted(actual_result) == [101, 101, 101]
    assert exec_result is None
    actual_result = []
