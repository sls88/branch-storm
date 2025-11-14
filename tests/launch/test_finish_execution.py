import re
from dataclasses import dataclass
from functools import wraps
from typing import Any, Union

import pytest

from src.branch_storm import typed_alias, RunConfigurations
from src.branch_storm.constants import STOP_CONSTANT
from src.branch_storm.launch_operations.errors import ConditionNotMetError
from src.branch_storm.operation import Operation as op, CallObject as obj
from src.branch_storm.branch import Branch as br
from src.branch_storm.type_containers import MandatoryArgTypeContainer as m

counter = 0


def read() -> int: return 1
def transform(arg: int) -> int: return arg + 1

def write(arg: int) -> None:
    global counter
    counter += 1
    return arg


def read_return_none(): return None
def return_one(): return 1
def read_arg_return_one(arg: int): return 1
def transform_return_none(arg: int): return None
def return_stop_constant(arg: int) -> Union[Any, str]:
    return STOP_CONSTANT


def test_stop_after_stop_constant():
    actual_result = br("trusted_to_enriched")[
        obj(return_one)(),
        obj(return_stop_constant)(m[int]),
        obj(write)(m[int])
    ].run()

    assert actual_result is None
    global counter
    assert counter == 0
    counter = 0


def test_stop_constant_last():
    actual_result = br("trusted_to_enriched")[
        obj(return_one)(),
        obj(return_stop_constant)(m[int]),
    ].run()

    assert actual_result is None


def test_fail_condition_last():
    actual_result = br("trusted_to_enriched")[
        obj(return_one)(),
        op(obj(transform)(m[int])).end_chain_if(lambda x: x == 1),
    ].run()

    assert actual_result is None


def test_continue_after_stop_constant():
    actual_result = br("trusted_to_enriched")[
        obj(return_one)(),
        obj(return_stop_constant)(m[int]),
        op(obj(return_one)()).force_call
    ].run()

    assert actual_result == 1


def test_stop_after_fail_condition():
    actual_result = br("trusted_to_enriched")[
        obj(read_return_none)(),
        op(obj(transform)(m[int])).end_chain_if(lambda x: x is None),
        op(obj(write)(m[int]))
    ].run()

    assert actual_result is None
    global counter
    assert counter == 0
    counter = 0


def test_return_stop_constant_in_nested_branch_catch_in_next_operation():
    actual_result = br("trusted_to_enriched")[
            op(obj(read)()),
            br("br1")[
                op(obj(return_stop_constant)(m[int])),
                op(obj(transform)(m[int]))
            ],
            op(obj(return_one)()).force_call
        ].run()

    global counter
    assert actual_result == 1


def test_return_stop_constant_in_nested_branch_last_op_not_run():
    actual_result = br("trusted_to_enriched")[
            op(obj(read)()),
            br("br1")[
                op(obj(return_stop_constant)(m[int])),
                op(obj(transform)(m[int])).op_name("after")
            ],
            obj(write)(m[int])
        ].run()

    assert actual_result is None
    global counter
    assert counter == 0
    counter = 0


def test_branch_after_end_chain_operation():
    actual_result = br("trusted_to_enriched")[
            obj(read)(),
            op(obj(transform)(m[int])).end_chain_if(lambda x: x == 1),
            br("br1")[
                obj(read)(),
                obj(write)(m[int])
            ],
            obj(write)(m[int])
        ].run()

    assert actual_result is None
    global counter
    assert counter == 0
    counter = 0


def test_branch_after_stop_constant_operation():
    actual_result = br("trusted_to_enriched")[
            obj(read)(),
            obj(return_stop_constant)(m[int]),
            br("br1")[
                obj(read)(),
                obj(write)(m[int])
            ],
            obj(write)(m[int])
        ].run()

    assert actual_result is None
    global counter
    assert counter == 0
    counter = 0


def test_branch_after_stop_constant_catch():
    actual_result = br("trusted_to_enriched")[
            obj(read)(),
            obj(return_stop_constant)(m[int]),
            br("br1")[
                obj(read)(),
                obj(write)(m[int])
            ].force_call,
            op(obj(write)(m[int])).end_chain_if(lambda x: x == 1)
        ].run()

    assert actual_result is None
    global counter
    assert counter == 1
    counter = 0


def test_branch_after_end_chain_operation_force_call():
    actual_result = br("trusted_to_enriched")[
            op(obj(read)()),
            op(obj(transform)(m[int])).end_chain_if(lambda x: x == 1),
            br("br1")[
                obj(read)(),
                obj(write)(m[int])
            ].force_call,
            obj(write)(m[int])
        ].run()

    assert actual_result == 1
    global counter
    assert counter == 2
    counter = 0


def test_return_stop_in_branch_second_not_run():
    actual_result = br("trusted_to_enriched")[
            br("br1")[
                op(obj(read)()).op_name("f1"),
                op(obj(return_stop_constant)(m[int])).op_name("f2"),
                op(obj(write)(m[int])).op_name("f3")
            ],
            br("br2")[
                op(obj(read)()).op_name("f4"),
                op(obj(transform)(m[int])).op_name("f5"),
                op(obj(write)(m[int])).op_name("f6")
            ]
        ].run()

    assert actual_result is None
    global counter
    assert counter == 0
    counter = 0


def test_return_stop_constant_in_branch_catch_in_next_branch_third_will_run():
    actual_result = br("trusted_to_enriched")[
            br("br1")[
                op(obj(read)()).op_name("f1"),
                op(obj(return_stop_constant)(m[int])).op_name("f2"),
                op(obj(write)(m[int])).op_name("f3")
            ],
            br("br2")[
                op(obj(read)()).op_name("f4"),
                op(obj(transform)(m[int])).op_name("f5"),
                op(obj(write)(m[int])).op_name("f6")
            ].force_call,
            br("br3")[
                op(obj(transform)(m[int])).op_name("f7"),
                op(obj(transform)(m[int])).op_name("f8"),
                op(obj(write)(m[int])).op_name("f9")
            ]
        ].run()

    assert actual_result == 4
    global counter
    assert counter == 2
    counter = 0


def test_fail_cond_in_nested_branch_last_stop():
    actual_result = br("trusted_to_enriched")[
        br("br1")[
            br("br2")[
                op(obj(read)()).op_name("f1"),
                op(obj(transform)(m[int])).op_name("f2").end_chain_if(lambda x: x == 1),
                op(obj(write)(m[int])).op_name("f3")
            ],
            op(obj(transform)(m[int])).op_name("f4"),
            op(obj(write)(m[int])).op_name("f5")
        ]
    ].run()

    assert actual_result is None
    global counter
    assert counter == 0
    counter = 0


def test_fail_cond_in_nested_branch_catch_in_previous():
    actual_result = br("trusted_to_enriched")[
        br("br1")[
            br("br2")[
                obj(read)(),
                op(obj(transform)(m[int])).end_chain_if(lambda x: x == 1),
                obj(write)(m[int])
            ],
            op(obj(read)()).force_call,
            obj(write)(m[int])
        ]
    ].run()

    assert actual_result == 1
    global counter
    assert counter == 1
    counter = 0


def test_raise_err_if_none_neg():
    with pytest.raises(
            ConditionNotMetError,
            match=re.escape(
                "Operation: trusted_to_enriched -> transform().\n"
                "Received data: (<class 'NoneType'>,)(their types show)")):
        br("trusted_to_enriched")[
            op(obj(read_return_none)()),
            op(obj(transform)(m[int])).raise_err_if(lambda x: x is None),
            op(obj(write)(m[int]))
        ].run()

    global counter
    assert counter == 0
    counter = 0


def test_stop_in_nested_branch_last():
    actual_result = br("trusted_to_enriched")[
            op(obj(read)()),
            br("br1")[
                op(obj(read_arg_return_one)(m[int])),
                op(obj(transform)(m[int])).end_chain_if(lambda x: x == 1)
            ],
            op(obj(write)(m[int]))
        ].run()

    global counter
    assert actual_result is None
    assert counter == 0
    counter = 0


def test_return_stop_constant_in_nested_branch():
    actual_result = br("trusted_to_enriched")[
            op(obj(read)()),
            br("br1")[
                op(obj(transform)(m[int])),
                op(obj(return_stop_constant)(m[int])),
                op(obj(transform)(m[int]))
            ],
            op(obj(write)(m[int]))
        ].run()

    global counter
    assert actual_result is None
    assert counter == 0
    counter = 0


def test_raise_err_after_end_execution():
    actual_result = br("br1")[
            op(obj(transform)(m[int])).end_chain_if(lambda x: x == 1),
            op(obj(transform)(m[int])).raise_err_if(
                lambda x: not isinstance(x, int))
        ].run(1)

    global counter
    assert actual_result is None
    assert counter == 0
    counter = 0


def test_hide_log_inf():
    actual_result = br("trusted_to_enriched")[
        op(obj(read)()),
        op(obj(transform)(m[int])),
        br("br1")[
            br("br2")[
                op(obj(transform)(m[int])).op_name("show1"),
                br("br3")[
                    op(obj(transform)(m[int])),
                    op(obj(transform)(m[int])).hide_log_inf(all_inf=True),
                    op(obj(transform)(m[int])).op_name("show2").hide_log_inf(False),
                ].hide_log_inf(True),
                op(obj(transform)(m[int])) #"show3"
            ].hide_log_inf(False),
            op(obj(transform)(m[int])),
            op(obj(transform)(m[int])).op_name("show4").hide_log_inf(False),
            op(obj(transform)(m[int]))
        ],
        op(obj(transform)(m[int]))
    ].hide_log_inf(True).run()

    assert actual_result == 11


def test_stop_execution_with_decorator_and_branch_break():
    @dataclass
    class JA:
        err_mess: str = "error"

    def catch_exception(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                return kwargs["job_args"].err_mess
        return wrapper

    @catch_exception
    def wrapped_func(err: bool, *, job_args: JA):
        if err is True:
            raise TypeError
        else:
            return STOP_CONSTANT

    ja = typed_alias("ja", JA)
    run_conf = typed_alias("run_conf", RunConfigurations)

    def calc_nested_br(run_conf: RunConfigurations):
        res = br("nested")[
            obj(wrapped_func)(False, job_args=m(ja)[JA])
        ].rw_inst({"run_conf": run_conf}).run()
        res: RunConfigurations = res[2]
        res = res.get_rw_inst()["ja"].err_mess
        return res

    actual_result = br("decorator_test")[
        obj(calc_nested_br)(m(run_conf)),
    ].rw_inst({"ja": JA()}).run()

    assert actual_result == "error"