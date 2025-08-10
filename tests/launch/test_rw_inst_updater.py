import re
from typing import Any, Dict, Optional

import pytest

from src.branch_storm.default.rw_classes import Variables, Values, RunConfigurations
from src.branch_storm.default.rw_classes import RwInstUpdater


class A:
    pass


class B:
    pass


class C:
    pass


a = A()
b = B()
c = C()
run_conf = RunConfigurations()


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

        ({"variables": Variables(), "val": Values(), "a": a, "run_conf": run_conf}, {},
            {"variables": Variables(), "val": Values(), "run_conf": run_conf}),

        ({"variables": Variables(), "val": Values(), "a": a, "run_conf": run_conf}, {"a": "del"},
         {"variables": Variables(), "val": Values(), "run_conf": run_conf}),

        ({"variables": Variables(), "val": Values(), "a": a, "run_conf": run_conf}, {"a": "b"},
         {"variables": Variables(), "val": Values(), "b": a, "run_conf": run_conf}),

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
    stack = "br1 -> br2"
    actual_result = RwInstUpdater()._get_updated(
        stack, current_rw_inst, rw_inst_from_option)
    expected_result = RwInstUpdater._assign_stack_for_def_cl(stack, expected_result)

    assert actual_result == expected_result


def test_clean():
    values_with_field = Values()
    values_with_field.field = 1
    variables_with_field = Variables()
    variables_with_field.field = 2
    stack = "br1 -> br2"
    current_rw_inst = {"var": variables_with_field, "val": values_with_field}
    rw_inst_from_option = {"val": "clean", "var": "clean"}
    actual_result = RwInstUpdater()._get_updated(
        stack, current_rw_inst, rw_inst_from_option)
    with pytest.raises(
            AttributeError,
            match='Operation: br1 -> br2. No such attribute in Values'):
        actual_result['val'].field
    with pytest.raises(
            AttributeError,
            match='Operation: br1 -> br2. No such attribute in Variables'):
        actual_result['var'].field


def test_clean_neg():
    values_with_field = Values()
    values_with_field.field = 1
    stack = "br1 -> br2"
    current_rw_inst = {"var": Variables(), "val": values_with_field, "a": a}
    rw_inst_from_option = {"a": "ClEan"}
    with pytest.raises(
            TypeError,
            match=re.escape(
                "Operation: br1 -> br2. You can get a new instance by executing command: "
                "'clean', 'new', 'new_inst', 'new_instance'\nonly for default rw instance "
                "Values(), Variables().")):
        RwInstUpdater()._get_updated(
            stack, current_rw_inst, rw_inst_from_option)


def test_clean_run_conf_neg():
    stack = "br1 -> br2"
    current_rw_inst = {"var": Variables(), "val": Values(), "run_conf": run_conf}
    rw_inst_from_option = {"run_conf": "clean"}
    with pytest.raises(
            TypeError,
            match=re.escape(
                "Operation: br1 -> br2. You can get a new instance by executing command: "
                "'clean', 'new', 'new_inst', 'new_instance'\nonly for default rw instance "
                "Values(), Variables().")):
        RwInstUpdater()._get_updated(
            stack, current_rw_inst, rw_inst_from_option)


def test_drop_default_rw_neg():
    stack = "br1 -> br2"
    current_rw_inst = {"var": Variables(), "val": Values(), "a": a}
    rw_inst_from_option = {"val": "drop"}
    with pytest.raises(
            TypeError,
            match=re.escape(
                f"Operation: br1 -> br2. "
                f"You cannot delete instances of default classes Values(), Variables(),\n"
                    f"but they can be completely replaced with a new instance by the "
                    f"'clean', 'new', 'new_inst', 'new_instance' commands.\n"
                    f"Also, you cannot delete or 'clean' by the command default RunConfiguratons()")):
        RwInstUpdater()._get_updated(
            stack, current_rw_inst, rw_inst_from_option)


def test_drop_rw_conf_neg():
    stack = "br1 -> br2"
    current_rw_inst = {"var": Variables(), "val": Values(), "run_conf": run_conf}
    rw_inst_from_option = {"run_conf": "drop"}
    with pytest.raises(
            TypeError,
            match=re.escape(
                f"Operation: br1 -> br2. "
                f"You cannot delete instances of default classes Values(), Variables(),\n"
                    f"but they can be completely replaced with a new instance by the "
                    f"'clean', 'new', 'new_inst', 'new_instance' commands.\n"
                    f"Also, you cannot delete or 'clean' by the command default RunConfiguratons()")):
        RwInstUpdater()._get_updated(
            stack, current_rw_inst, rw_inst_from_option)


def test_rename_run_conf_rw_neg():
    stack = "br1 -> br2"
    current_rw_inst = {"var": Variables(), "val": Values(), "run_conf": run_conf}
    rw_inst_from_option = {"run_conf": "config"}
    with pytest.raises(
            TypeError,
            match=re.escape(
                f"Operation: br1 -> br2. "
                f"The default rw instance RunConfiguratons() can only have 'run_conf' alias")):
        RwInstUpdater()._get_updated(
            stack, current_rw_inst, rw_inst_from_option)


def test_add_run_conf_with_another_name_rw_neg():
    stack = "br1 -> br2"
    current_rw_inst = {"var": Variables(), "val": Values()}
    rw_inst_from_option = {"run_config": run_conf}
    with pytest.raises(
            TypeError,
            match=re.escape(
                f"Operation: br1 -> br2. "
                f"The default rw instance RunConfiguratons() can only have 'run_conf' alias")):
        RwInstUpdater()._get_updated(
            stack, current_rw_inst, rw_inst_from_option)


def test_get_updated_all():
    current_rw_inst = {"var": Variables(), "val": Values(), "run_conf": run_conf}
    rw_inst_from_option = (None, {"a": a}, {"b": b})

    actual_result = RwInstUpdater.get_updated_all(
        "stack", current_rw_inst, rw_inst_from_option)

    assert actual_result == {
        'var': Variables(_op_stack_name='stack'),
        'val': Values(_op_stack_name='stack'),
        'run_conf': run_conf,
        'a': a,
        'b': b}
