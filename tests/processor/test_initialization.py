from inspect import Parameter
from typing import Union

import pytest

from src.branch_storm.initialization_core import get_args_from_arg_type, get_first_element, replace_and_get_elem_by_pos
from src.branch_storm.type_containers import MandatoryArgTypeContainer as m, OptionalArgTypeContainer as opt


@pytest.mark.parametrize(
    ("type_container", "expected_result"),
    [
        (m, None),
        (opt, None),
        (m[str], str),
        (opt[str], str),
        (m(1)[bool], bool),
        (opt(1)[bool], bool),
        (m(1), None),
        (opt(1), None),
        (m(seq=True), None),
        (opt(seq=True), None),
        (m(seq=True)[Union[int, str]], Union[int, str]),
        (opt(seq=True)[Union[int, str]], Union[int, str])
    ],
)
def test_get_args_from_arg_type(type_container, expected_result):
    actual_result = get_args_from_arg_type(type_container)

    assert actual_result == expected_result


@pytest.mark.parametrize(
    ("input_data", "expected_result"),
    [
        ((1, 2, 3), (1, (2, 3))),
        ((1,), (1, ())),
        ((), (Parameter.empty, ()))
    ],
)
def test_get_element(input_data, expected_result):
    actual_result = get_first_element(input_data)

    assert actual_result == expected_result


@pytest.mark.parametrize(
    ("input_data", "elem_pos", "expected_result"),
    [
        ((1, 2, 3), 2, (2, (1, "uniq_id", 3))),
        ((1, 2, 3), 1, (1, ("uniq_id", 2, 3))),
        ((1, 2, 3), 3, (3, (1, 2, "uniq_id"))),
        ((1, 2, 3), 4, (Parameter.empty, (1, 2, 3))),
        ((1, 2, 3), 0, (Parameter.empty, (1, 2, 3))),
        ((1, 2, 3), -1, (Parameter.empty, (1, 2, 3))),
        ((), 0, (Parameter.empty, ())),
        ((), 1, (Parameter.empty, ())),
    ],
)
def test_get_element(input_data, elem_pos, expected_result):
    actual_result = replace_and_get_elem_by_pos(input_data, elem_pos, "uniq_id")

    assert actual_result == expected_result
