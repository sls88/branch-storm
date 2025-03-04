from dataclasses import dataclass
from typing import Tuple

import pytest

from _src.default.rw_classes import Values, Variables
from _src.operation import Operation as op, CallObject as obj
from _src.branch import Branch as br
from _src.type_containers import MandatoryArgTypeContainer as m
from _src.utils.common import renew_def_rw_inst


def return_one(): return 1
def get_and_pass_args(*arg: int) -> int: return arg


def get_and_return_tuple_two_values(arg1: int, arg2: int) -> Tuple[int, int]:
    return arg1, arg2


def get_arg_and_get_rw_class_value_via_object_kwargs(arg: int, rw_value: int) -> Tuple[int, int]:
    return arg, rw_value

@dataclass
class ThirdStorage:
    third_val: int = None

@dataclass
class Storage:
    t_class: ThirdStorage = None

    def __post_init__(self):
        self.t_class = ThirdStorage()


def test_assign_default_rw_class_vals_vars():
    operation = op(obj(get_and_pass_args)(1, 2, 3)).assign(
        "val.first_val", "var.second_val", "st.t_class.third_val")
    result = operation.rw_inst({"st": Storage()}).run(())
    actual_result, actual_rem_args = result

    assert actual_result[0].first_val == 1
    assert actual_result[1].second_val == 2
    assert actual_result[2].t_class.third_val == 3
    assert actual_rem_args is None


def test_renew_rw_inst():
    val = Values()
    val.val_field1 = 1
    val.val_field2 = 2
    var = Variables()
    var.var_field1 = 1
    st = Storage()
    st.t_class.third_val = 3

    rw_inst = {"val": val, "var": var, "st": st}

    base_id = {k: id(v) for k, v in rw_inst.items()}
    rw_inst = renew_def_rw_inst("st -> ack", rw_inst)
    actual_result_id = {k: id(v) for k, v in rw_inst.items()}

    actual_fields = {k: v.__dict__ for k, v in rw_inst.items()}
    expected_fields = {'val': {'_op_stack_name': '', 'val_field1': 1, 'val_field2': 2},
                       'var': {'_op_stack_name': '', 'var_field1': 1},
                       'st': {'t_class': ThirdStorage(third_val=3)}}

    assert base_id["val"] != actual_result_id["val"]
    assert base_id["var"] != actual_result_id["var"]
    assert base_id["st"] == actual_result_id["st"]
    assert actual_fields == expected_fields


def test_assign_default_rw_class_vals_vars_via_opr_assign():
    actual_result = br("trusted_to_enriched")[
        op(obj(return_one)()).assign("val.store_one"),
        obj(return_one)(),
        obj(get_and_pass_args)(m[int]),
        br("br1")[
            obj(get_and_pass_args)(m[int]),
            op(obj(get_arg_and_get_rw_class_value_via_object_kwargs)(
            m[int], rw_value=m("val.store_one")[int])).assign(
            "var.first_var", "var.second_var"),
        ],
        obj(get_and_return_tuple_two_values)(arg1=m("var.first_var")[int], arg2=m("var.second_var")[int]),
        obj(get_and_return_tuple_two_values)(m[int], m[int])
    ].run()

    assert actual_result == (1, 1)


def test_assign_default_rw_class_vals_vars_via_opr_neg():
    with pytest.raises(
            AttributeError,
            match="Operation: trusted_to_enriched -> get_arg_and_get_rw_class_value_via_object_kwargs. "
                  "No such attribute in Variables"):
        br("trusted_to_enriched")[
            br("br1")[
                op(obj(return_one)()).assign("val.store_one"),
                obj(return_one)(),
                obj(get_and_pass_args)(m[int]),
            ],
            op(obj(get_arg_and_get_rw_class_value_via_object_kwargs)(
                m[int], rw_value=m("val.store_one")[int])).assign(
                "var.first_var", "var.second_var"),
            obj(get_and_return_tuple_two_values)(arg1=m("var.first_var")[int],
                                                 arg2=m("var.second_var")[int]),
            obj(get_and_return_tuple_two_values)(m[int], m[int])
        ].run()
