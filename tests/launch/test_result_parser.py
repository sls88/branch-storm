from dataclasses import dataclass
from typing import Any, Dict, Tuple

import pytest

from _src.constants import STOP_CONSTANT
from _src.launch_operations.data_parsing import ResultParser


@dataclass
class TransitClass:
    new_instance: bool = False


@dataclass
class SecondTransitClass:
    new_instance: bool = False


tc = TransitClass()
stc = SecondTransitClass()
new_tc = TransitClass(new_instance=True)
new_stc = SecondTransitClass(new_instance=True)


@pytest.mark.parametrize(
    ("data", "rw_inst", "expected_result"),
    [
        ((), {"tc": tc}, ((), False, {"tc": tc})),
        (((),), {"tc": tc}, (((),), False, {"tc": tc})),
        ((None,), {"tc": tc}, ((None,), False, {"tc": tc})),
        ((STOP_CONSTANT,), {"tc": tc}, ((), True, {"tc": tc})),
        ((new_tc,), {"tc": tc}, ((), False, {"tc": new_tc})),
        ((new_tc, STOP_CONSTANT), {"tc": tc}, ((), True, {"tc": new_tc})),
        ((1, new_tc), {"tc": tc}, ((1,), False, {"tc": new_tc})),
        (((), new_tc), {"tc": tc}, (((),), False, {"tc": new_tc})),
        ((None, new_tc), {"tc": tc}, ((None,), False, {"tc": new_tc})),
        ((1, new_tc, 2), {"tc": tc}, ((1, 2), False, {"tc": new_tc})),
        ((1, new_tc, 2, new_stc), {"tc": tc, "stc": stc},
            ((1, 2), False, {"tc": new_tc, "stc": new_stc})),
        ((1, new_tc, 2, new_stc, None, ()), {"tc": tc, "stc": stc},
            ((1, 2, None, ()), False, {"tc": new_tc, "stc": new_stc})),
        ((1, new_tc, 2, STOP_CONSTANT, new_stc), {"tc": tc, "stc": stc},
         ((1, 2), True, {"tc": new_tc, "stc": new_stc})),
    ],
)
def test_sort_data(data: Tuple, rw_inst: Dict[str, Any], expected_result: Tuple[Tuple, Dict[str, Any]]):
    sd, new_rw_inst = ResultParser.sort_data(data, rw_inst)
    actual_result = sd.data, sd.stop_all_operations, new_rw_inst

    assert actual_result == expected_result
