from dataclasses import dataclass
from typing import Optional

from src.branch_storm.default.rw_classes import RunConfigurations, Values, Variables, BranchOptions


@dataclass
class A:
    field: Optional[str] = None

@dataclass
class B:
    field: Optional[str] = None


def test_add_read_branch_stack():
    run_conf = RunConfigurations()
    run_conf.add_br_opt_to_stack(BranchOptions(br_name="br1"))
    add_1 = run_conf.get_branch_stack()

    run_conf.add_br_opt_to_stack(BranchOptions(br_name="br2"))
    add_2 = run_conf.get_branch_stack()

    run_conf.add_br_opt_to_stack(BranchOptions())
    add_3 = run_conf.get_branch_stack()

    run_conf.add_br_opt_to_stack(BranchOptions(br_name="br3"))
    add_4 = run_conf.get_branch_stack()

    actual_result = [add_1, add_2, add_3, add_4]
    assert actual_result == [
        'br1',
        'br1 -> br2',
        'br1 -> br2 -> BRANCH NAME NOT DEFINED',
        'br1 -> br2 -> BRANCH NAME NOT DEFINED -> br3']


def test_add_read_operation_stack():
    run_conf = RunConfigurations()
    run_conf.add_br_opt_to_stack(BranchOptions(br_name="br1"))
    run_conf.set_operation_stack("operation")
    actual_op_stack = run_conf.operation_stack

    run_conf.set_operation_stack("operation2")
    actual_second_op_stack = run_conf.operation_stack

    run_conf.add_br_opt_to_stack(BranchOptions(br_name="br2"))
    run_conf.set_operation_stack("operation3")
    actual_third_op_stack = run_conf.operation_stack

    assert actual_op_stack == "br1 -> operation"
    assert actual_second_op_stack == "br1 -> operation2"
    assert actual_third_op_stack == "br1 -> br2 -> operation3"


def test_add_options_stack():
    run_conf = RunConfigurations()
    run_conf.add_br_opt_to_stack(BranchOptions(
        br_name="br1", hide_log_inf=(None, True), distribute_input_data=False))

    actual_first_add = (
        run_conf.br_opt.hide_log_inf,
        run_conf.br_opt.check_type_strategy_all,
        run_conf.br_opt.distribute_input_data,
        run_conf.br_opt.delayed_return)

    run_conf.br_opt.delayed_return = (1,)
    run_conf.add_br_opt_to_stack(BranchOptions(
        br_name="br2", check_type_strategy_all=False, distribute_input_data=False))

    actual_second_add = (
        run_conf.br_opt.hide_log_inf,
        run_conf.br_opt.check_type_strategy_all,
        run_conf.br_opt.distribute_input_data,
        run_conf.br_opt.delayed_return)

    assert actual_first_add == ((False, True), True, False, None)
    assert actual_second_add == ((False, True), False, False, ())


def test_add_rw_inst():
    run_conf = RunConfigurations()
    actual_initial = run_conf.get_rw_inst()

    run_conf.add_br_opt_to_stack(BranchOptions(
        br_name="br1", rw_inst=({"a": A(field=1)},)))
    run_conf.set_operation_stack("op1")

    actual_first_add = run_conf.get_rw_inst()

    run_conf.set_operation_stack("op2")
    run_conf.update_last_rw_inst(({"a": B(field=2)},))

    actual_updated = run_conf.get_rw_inst()
    actual_updated_in_stack = dict(run_conf.opt_stack[-1].rw_inst)['val']._op_stack_name


    assert actual_initial == {
        "val": Values(_op_stack_name='Initial (branch)'),
        "var": Variables(_op_stack_name='Initial (branch)'),
        "run_conf": run_conf}

    assert actual_first_add == {
        "val": Values(_op_stack_name='br1 -> op2'),
        "var": Variables(_op_stack_name='br1 -> op2'),
        "run_conf": run_conf,
        "a": A(field=1)}

    assert actual_updated == {
        "val": Values(_op_stack_name='br1 -> op2'),
        "var": Variables(_op_stack_name='br1 -> op2'),
        "run_conf": run_conf,
        "a": B(field=2)}

    assert actual_updated_in_stack == 'br1 -> op2'


def test_pop_stack():
    run_conf = RunConfigurations()

    run_conf.add_br_opt_to_stack(BranchOptions(
        br_name="br1", check_type_strategy_all=False, distribute_input_data=False,
        rw_inst=({"a": A(field=1)},)))
    run_conf.set_operation_stack("op1")
    run_conf.br_opt.delayed_return = ()

    run_conf.set_operation_stack("op2")
    run_conf.update_last_rw_inst(({"ab": A(field=2)},))
    run_conf.br_opt.delayed_return = (1,)

    val = Values()
    val.cust_field=100
    run_conf.add_br_opt_to_stack(BranchOptions(
        br_name="br2", hide_log_inf=(True, None), check_type_strategy_all=True,
        distribute_input_data=False, rw_inst=({"abc": A(field=3), "b": B(field=1), "val": val},)))
    run_conf.set_operation_stack("op3")
    run_conf.br_opt.delayed_return = (2, 3)

    actual_opt_hide_before_pop = run_conf.br_opt.hide_log_inf
    actual_opt_check_before_pop = run_conf.br_opt.check_type_strategy_all
    actual_delayed_return_before_pop = run_conf.br_opt.delayed_return
    actual_rw_inst_before_pop = run_conf.get_rw_inst()

    run_conf.pop_stack()

    actual_opt_hide_after_pop = run_conf.br_opt.hide_log_inf
    actual_opt_check_after_pop = run_conf.br_opt.check_type_strategy_all
    actual_delayed_return_after_pop = run_conf.br_opt.delayed_return
    actual_rw_inst_after_pop = run_conf.get_rw_inst()

    run_conf.pop_stack()

    actual_opt_hide_after_second_pop = run_conf.br_opt.hide_log_inf
    actual_opt_check_after_second_pop = run_conf.br_opt.check_type_strategy_all
    actual_delayed_return_after_second_pop = run_conf.br_opt.delayed_return
    actual_rw_inst_after_second_pop = run_conf.get_rw_inst()

    actual_result = run_conf.get_rw_inst()["val"].cust_field

    assert actual_rw_inst_before_pop == {
        'val': val,
        'var': Variables(_op_stack_name='br1 (branch)'),
        'run_conf': run_conf,
        'abc': A(field=3),
        'b': B(field=1)}

    assert actual_rw_inst_after_pop == {
        'val': val,
        'var': Variables(_op_stack_name='br1 (branch)'),
        'run_conf': run_conf,
        'ab': A(field=3)}

    assert actual_rw_inst_after_second_pop == {
        'val': val,
        'var': Variables(_op_stack_name='br1 (branch)'),
        'run_conf': run_conf}

    assert actual_result == 100

    assert actual_opt_hide_before_pop == (True, False)
    assert actual_opt_check_before_pop == True
    assert actual_opt_hide_after_pop == (False, False)
    assert actual_opt_check_after_pop == False
    assert actual_opt_hide_after_second_pop == (False, False)
    assert actual_opt_check_after_second_pop == True

    assert actual_delayed_return_before_pop == (2, 3)
    assert actual_delayed_return_after_pop == (1, 2, 3)
    assert actual_delayed_return_after_second_pop is None
