from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import pytest

from src.branch_storm.branch import get_run_config
from src.branch_storm.default.rw_classes import RunConfigurations


run_conf1 = RunConfigurations()
run_conf2 = RunConfigurations()

id1 = id(run_conf1)
id2 = id(run_conf2)
id3 = "id3"

@dataclass
class A:
    pass

@dataclass
class B:
    pass

@dataclass
class C:
    pass

@pytest.mark.parametrize(
    ("rw_inst_opt", "input_data", "expected_result"),
    [
        (({"a": A(), "b": B(), "run_conf": run_conf1},), ("h", run_conf2, 1),
         (id2, ('h', 1))),
        (({"a": A()}, {"b": B()}), ("h", run_conf1, run_conf2, 1),
         (id1, ('h', 1))),
        ((), ("h", run_conf2, run_conf1, 1),
         (id2, ('h', 1))),
        ((), (run_conf2, ()),
         (id2, ())),
        ((), (run_conf2, 1),
         (id2, 1)),
        ((), (run_conf2,),
         (id2, ())),
        ((), run_conf1,
         (id1, None)),
        (({"run_conf": run_conf1},), None,
         (id1, None)),
        (({"a": A(), "b": B()},), ("h", [], 1),
         (id3, ('h', [], 1))),
        (({"a": A(), "b": B(), "run_conf": run_conf1},), 1,
         (id1, 1)),
        (({"a": A(), "b": B(), "run_conf": run_conf1},), None,
         (id1, None)),
        (({"a": A(), "b": B()},), B(),
         (id3, B())),
        ((), None,
         (id3, None)),
        (({"a": A(), "b": B()}, {"c": C(), "run_conf": run_conf1}), ("h", run_conf2, 1),
         (id2, ('h', 1))),
        (({"a": A(), "b": B()}, {"c": C(), "run_conf": run_conf1}), ["h", run_conf2, 1],
         (id1, ['h', run_conf2, 1])),
        (({"a": A(), "b": B()}, {"c": C()}), ["h", run_conf2, 1],
         (id3, ['h', run_conf2, 1])),
    ],
)
def test_get_run_config(
        rw_inst_opt: Tuple[Dict[str, Any]],
        input_data: Optional[Any],
        expected_result: Tuple[int, Any]):
    actual_result_id, actual_result_rest_data = get_run_config(rw_inst_opt, input_data)
    actual_result_id = id(actual_result_id)

    expected_result_id, expected_result_rest_data = expected_result

    if expected_result_id == id3:
        assert actual_result_id != id1 and actual_result_id != id2
    else:
        assert actual_result_id == expected_result_id
    assert actual_result_rest_data == expected_result_rest_data
