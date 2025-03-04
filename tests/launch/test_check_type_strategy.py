import re
from typing import Tuple, List

import pytest
from _src.operation import Operation as op, CallObject as obj
from _src.branch import Branch as br
from _src.type_containers import MandatoryArgTypeContainer as m, OptionalArgTypeContainer as opt


def return_1_2_str3() -> Tuple[int, int, str]: return [1, 2, "3"]
def receive_and_pass_all(*args) -> Tuple: return args


def test_check_type_strategy_first():
    actual_result = br("enriched_job")[
        obj(return_1_2_str3)(),
        op(obj(receive_and_pass_all)(m[List[int]])).check_type_strategy_all(False),
    ].run()

    assert actual_result == ([1, 2, '3'],)


def test_check_type_strategy_all_by_default_neg():
    with pytest.raises(
            TypeError,
            match=re.escape(
                "Operation: enriched_job -> receive_and_pass_all.\n"
                "Argument mismatches with their types were found:\n"
                "Len: 1; Arg type map: {1: (<class 'list'>, typing.List[int])}")):
        br("enriched_job")[
            obj(return_1_2_str3)(),
            op(obj(receive_and_pass_all)(m[List[int]])),
        ].run()


def test_check_type_strategy_nested_neg():
    with pytest.raises(
            TypeError,
            match=re.escape(
                "Operation: enriched_job -> br1 -> receive_and_pass_all.\n"
                "Argument mismatches with their types were found:\n"
                "Len: 1; Arg type map: {1: (<class 'list'>, typing.List[int])}")):
        br("enriched_job")[
            obj(return_1_2_str3)(),
            obj(receive_and_pass_all)(m[List[int]]),
            br("br1")[
                op(obj(receive_and_pass_all)(m[List[int]])).check_type_strategy_all(True),
            ]
        ].check_type_strategy_all(False).run()
