import re
from typing import Tuple, Any, Optional, List

import pytest

from src.branch_storm.default.rw_classes import RunConfigurations
from src.branch_storm.launch_operations.capture_manager import register_ops
from src.branch_storm.operation import Operation as op, CallObject as obj
from src.branch_storm.branch import Branch as br, BrRecursiveProcessor, \
    Processor, _BrShared
from src.branch_storm.type_containers import MandatoryArgTypeContainer as m, OptionalArgTypeContainer as opt


def return_int_one() -> int: return 1
def return_tuple_args() -> Tuple[int, int]: return 1, 2
def get_int_arg_and_plus_one(arg: int) -> int: return arg + 1
def func_nested(arg: int) -> int: return arg + 1
def deep_func_nested(arg: int) -> int: return arg + 1
def get_one_arg_return_two(arg: int) -> Tuple[int, int]:
    return arg + 1, 1
def return_none_inp_arg(arg: int) -> None: return None
def return_none_wo_arg() -> None: return None
def pass_one_arg(arg: int) -> int: return arg
def pass_two_args(arg1: int, arg2: int) -> Tuple[int, int]: return arg1, arg2
def summ_result(arg: int, result: int) -> int: return arg + result


def test_run_one_operation():
    actual_result = br("enriched_job")[
        obj(return_int_one)()
    ].run()

    assert actual_result == 1


def test_run_two_operations():
    actual_result = br("enriched_job")[
        obj(return_tuple_args)(),
        obj(pass_two_args)(m[int], m[int])
    ].run()

    assert actual_result == (1, 2)



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


def read(table_name): return table_name
def transform(arg): return f"Table: {arg}"
def write(arg) -> None:
    tn = f"{arg} has been written."
    global written_tables
    written_tables.append(tn)

written_tables = []


def test_process_two_branches():
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
        ],
    ].run()

    global written_tables
    assert written_tables == [
        "Table: dim_term has been written.",
        "Table: dim_pale has been written."]
    assert actual_result is None
    written_tables = []


def dim_term_br() -> br:
    return br("dim_term")[
            read(table_name="dim_term"),
            transform(m[str]),
            write(m[str]),
        ]

def dim_pale_kale_list_br() -> List[br]:
    return [
        br("dim_pale")[
            read("dim_pale"),
            transform(m[str]),
            write(m[str]),
        ],
        br("dim_kale")[
            read("dim_kale"),
            obj(transform)(m[str]),
            write(m[str]),
        ]
    ]


def test_process_three_branches_tuple():
    register_ops(read, transform, write)

    actual_result = br("trusted_to_enriched_job")(
        dim_term_br(),
        *dim_pale_kale_list_br()
    ).run()

    global written_tables
    assert written_tables == [
        "Table: dim_term has been written.",
        "Table: dim_pale has been written.",
        "Table: dim_kale has been written."]
    assert actual_result is None
    written_tables = []


def test_process_two_branches_error_field_in_def_rw_class():
    with pytest.raises(
            AttributeError,
            match=re.escape("Operation: trusted_to_enriched_job -> dim_pale "
                            "-> transform(). No such attribute in Values")):
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
            ],
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
                op(obj(return_none_wo_arg)()).rw_inst({"val": "clean"})
            ],
            br("dim_pale")[
                obj(read)("dim_pale"),
                op(obj(transform)(m[str])).assign("val.field_result"),
                obj(return_none_wo_arg)()
            ],
        ].run()

    assert actual_result is None


def test_process_two_branches_double_writing_in_def_rw_class_negative():
    with pytest.raises(
            ValueError,
            match=re.escape(
                "Operation: trusted_to_enriched_job -> dim_pale -> transform(). "
                "The value cannot be overwritten. The class is intended "
                "for single-write and read use.")):
        br("trusted_to_enriched_job")[
            br("dim_term")[
                op(obj(read)(table_name="dim_term")),
                op(obj(transform)(m[str])).assign("val.field_result"),
            ],
            br("dim_pale")[
                obj(read)("dim_pale"),
                op(obj(transform)(m[str])).assign("val.field_result"),
            ],
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
        ].end_chain_if(lambda x: x is None),
    ].run()

    global written_tables
    assert written_tables == [
        "Table: dim_term has been written."]
    assert actual_result is None
    written_tables = []


def calculate_deep_nested_branch(arg: int, run_conf: RunConfigurations) -> int:
    return br("d_nested_br")[
        obj(deep_func_nested)(m[int]),
        br("dd_nested_br")[
            op(obj(deep_func_nested)(m[int])).op_name("deep_deep_func_nested"),
            op(obj(deep_func_nested)(m[int])).op_name("deep_deep_func_nested2").assign("val.field"),
            obj(return_int_one)()
        ],
        op(obj(deep_func_nested)(m[int])).op_name("end_sep_func")
    ].rw_inst({"run_conf": run_conf}).run(arg)


def test_run_nested_functions_with_branch_break():
    actual_result = br("enriched_job")[
        obj(return_int_one)(),
        op(obj(get_int_arg_and_plus_one)(m[int])),
        br("nested_br")[
            obj(func_nested)(m[int]),
            obj(calculate_deep_nested_branch)(m[int], m("run_conf")),
            op(obj(func_nested)(m[int])).burn_rem_args.op_name("after_sep_func")
        ],
        obj(get_int_arg_and_plus_one)(m[int]),
        br("nested_br2")[
            obj(get_int_arg_and_plus_one)(m[int])
        ],
        obj(summ_result)(m[int], m("val.field"))
    ].run()

    assert actual_result == 11


custom_proc_run_counter = 0


class CustomProcessor(Processor):
    @staticmethod
    def run(operations,
            run_conf,
            input_data = None,
            rem_args = None
            ) -> Tuple[Optional[Any], Optional[Tuple], RunConfigurations]:
        global custom_proc_run_counter
        custom_proc_run_counter += 1

        result = input_data
        for operation in operations:
            operation = BrRecursiveProcessor._wrap_call_object(operation)
            if isinstance(operation, op):
                run_conf = operation._update_stack(run_conf)
                run_conf = operation._update_rw_inst(run_conf)
                next_operations_exist = True if operations else False
                result, rem_args, run_conf = _BrShared._execute_step(
                    True, next_operations_exist,
                    operation, run_conf, result)
            else:
                result, rem_args, run_conf = operation.rw_inst(
                    {"run_conf": run_conf}).run(result)
        return result, rem_args, run_conf


def test_custom_processor_second_branch():
    actual_result = br("trusted_to_enriched_job")[
        br("dim_term")[
            obj(read)(table_name="dim_term"),
            obj(transform)(m[str]),
            obj(write)(m[str]),
        ],
        br("dim_pale", CustomProcessor)[
            obj(read)("dim_pale"),
            obj(transform)(m[str]),
            obj(write)(m[str]),
        ],
        br("dim_kale")[
            obj(read)("dim_kale"),
            obj(transform)(m[str]),
            obj(write)(m[str]),
        ],
    ].run()

    global written_tables, custom_proc_run_counter
    assert custom_proc_run_counter == 1
    assert written_tables == [
        'Table: dim_term has been written.',
        'Table: dim_pale has been written.',
        'Table: dim_kale has been written.']
    assert actual_result is None
    written_tables = []
    custom_proc_run_counter = 0


def test_custom_processor_all_branches():
    actual_result = br("trusted_to_enriched_job", CustomProcessor)[
        br("dim_term")[
            obj(read)(table_name="dim_term"),
            obj(transform)(m[str]),
            obj(write)(m[str]),
        ],
        br("dim_pale")[
            obj(read)("dim_pale"),
            obj(transform)(m[str]),
            obj(write)(m[str]),
        ],
        br("dim_kale")[
            obj(read)("dim_kale"),
            obj(transform)(m[str]),
            obj(write)(m[str]),
        ],
    ].run()

    global written_tables, custom_proc_run_counter
    assert custom_proc_run_counter == 4
    assert written_tables == [
        'Table: dim_term has been written.',
        'Table: dim_pale has been written.',
        'Table: dim_kale has been written.']
    assert actual_result is None
    written_tables = []
    custom_proc_run_counter = 0


def test_custom_processor_in_nested_branches():
    actual_result = br("trusted_to_enriched_job")[
        br("dim_term")[
            obj(read)(table_name="dim_term"),
            obj(transform)(m[str]),
            obj(write)(m[str]),
        ],
        br("nested", CustomProcessor)[
            br("dim_pale")[
                obj(read)("dim_pale"),
                obj(transform)(m[str]),
                obj(write)(m[str]),
            ],
            br("dim_kale")[
                obj(read)("dim_kale"),
                obj(transform)(m[str]),
                obj(write)(m[str]),
            ],
        ],
        br("dim_fail")[
            obj(read)(table_name="dim_fail"),
            obj(transform)(m[str]),
            obj(write)(m[str]),
        ],
    ].run()

    global written_tables, custom_proc_run_counter
    assert custom_proc_run_counter == 3
    assert written_tables == [
        'Table: dim_term has been written.',
        'Table: dim_pale has been written.',
        'Table: dim_kale has been written.',
        'Table: dim_fail has been written.']
    assert actual_result is None
    written_tables = []
    custom_proc_run_counter = 0


def return_tuple_two_int() -> Tuple[int, int]:
    return 1, 2


def test_pass_two_args_in_parallel():
    actual_result = br("trusted_to_enriched_job")[
        obj(return_tuple_two_int)(),
        br("write")[
            op(obj(write)(m[int])).distribute_input_data,
            br("sub_write")[
                op(obj(pass_one_arg)(m[int])).end_chain_if(
                    lambda x: x == 3),
                obj(write)(m[int])
            ]
        ].take_all_args,
    ].run()

    global written_tables
    assert actual_result == (None, None)
    assert written_tables == ['1 has been written.', '2 has been written.']
    written_tables = []
