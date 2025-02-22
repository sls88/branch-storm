from typing import Any, Dict, Optional

import pytest

from src.default.rw_classes import Variables, Values
from src.launch_operations.rw_inst_updater import RwInstUpdater


class A:
    pass


class B:
    pass


class C:
    pass


a = A()
b = B()
c = C()


@pytest.mark.parametrize(
    ("current_rw_inst", "rw_inst_from_option", "expected_result"),
    [
        (None, {}, {"val": Values(), "var": Variables()}),
        ({}, None, {"val": Values(), "var": Variables()}),
        ({}, {}, {"val": Values(), "var": Variables()}),

        ({"values": Values()}, None,
            {"values": Values(), "var": Variables()}),

        ({"variables": Variables()}, None,
            {"val": Values(), "variables": Variables()}),

        (None, {"values": Values()},
            {"values": Values(), "var": Variables()}),

        (None, {"variables": Variables()},
            {"val": Values(), "variables": Variables()}),

        ({"values": Values()}, {"val": Values()},
            {"val": Values(), "var": Variables()}),

        ({"var": Variables()}, {"variables": Variables()},
            {"val": Values(), "variables": Variables()}),

        ({"variables": Variables()}, {"values": Values()},
            {"values": Values(), "variables": Variables()}),

        ({"variables": Variables(), "val": Values()}, {"values": Values()},
            {"values": Values(), "variables": Variables()}),

        ({"variables": Variables(), "val": Values()}, {},
            {"variables": Variables(), "val": Values()}),

        ({"variables": Variables(), "val": Values()}, {"var": Variables(), "values": Values()},
            {"values": Values(), "var": Variables()}),

        ({"variables": Variables(), "val": Values(), "a": a}, None,
            {"variables": Variables(), "val": Values(), "a": a}),

        ({"variables": Variables(), "val": Values(), "a": a}, {},
            {"variables": Variables(), "val": Values()}),

        ({"values": Values(), "a": a}, {"b": b},
         {"var": Variables(), "values": Values(), "a": a, "b": b}),

        ({"a": a}, {"a": b, "c": c},
         {"var": Variables(), "val": Values(), "a": b, "c": c}),

        ({"a": a, "b": b}, {"b": c},
         {"var": Variables(), "val": Values(), "a": a, "b": c}),

        ({"a": a, "b": b}, {"bb": b},
         {"var": Variables(), "val": Values(), "a": a, "bb": b}),

        (None, {"a": a},
         {"var": Variables(), "val": Values(), "a": a}),

        ({"a": a}, None,
         {"var": Variables(), "val": Values(), "a": a}),

        (None, None,
         {"var": Variables(), "val": Values()}),
    ],
)
def test_rw_inst_updater(
        current_rw_inst: Optional[Dict[str, Any]],
        rw_inst_from_option: Optional[Dict[str, Any]],
        expected_result: Dict[str, Any]):
    actual_result = RwInstUpdater().get_updated(
        "br1 -> br2", current_rw_inst, rw_inst_from_option)

    assert actual_result == expected_result
