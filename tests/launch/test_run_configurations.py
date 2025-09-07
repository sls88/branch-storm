from src.branch_storm.default.rw_classes import (
    RunConfigurations, BranchOptions, Values, Variables)

def find_aliases_by_type(m: dict, cls):
    return [k for k, v in m.items() if isinstance(v, cls)]

def pick_alias(m: dict, cls, prefer = None):
    aliases = find_aliases_by_type(m, cls)
    if not aliases:
        raise AssertionError(f"Alias for {cls.__name__} not found in map: {m.keys()}")
    if prefer and prefer in aliases:
        return prefer
    return aliases[0]

def make_rc_with_custom_stack():
    rc = RunConfigurations()

    bo_last = BranchOptions(
        br_name="B1",
        assign=("x",),
        force_call=True,
        rw_inst=({"cfg": {"x": 1}, "val2": Values()},))
    rc.add_br_opt_to_stack(bo_last)

    rc.set_operation_stack("DoStuff")

    m = rc.br_opt.rw_inst
    val_aliases = find_aliases_by_type(m, Values)
    var_alias = pick_alias(m, Variables, prefer="var")

    for i, a in enumerate(val_aliases, start=1):
        getattr(m, "keys", lambda: None)
        m[a].__setattr__(f"a{i}", (i, i+1))

    m[var_alias].num = 42
    sentinel = object()
    m[var_alias].obj = sentinel

    return rc


def test_returns_new_rc_and_keeps_stack_shape():
    rc1 = make_rc_with_custom_stack()
    rc2 = rc1.get_renewed_self_instance()

    assert rc1 is not rc2
    assert len(rc1.opt_stack) == len(rc2.opt_stack) == 2

    assert rc2.opt_stack[0] is rc1.opt_stack[0]

    assert rc2.opt_stack[-1] is not rc1.opt_stack[-1]
    assert rc2.br_opt is rc2.opt_stack[-1]

    assert rc2.br_opt.rw_inst is not rc1.br_opt.rw_inst
    assert rc2.br_opt.rw_inst["run_conf"] is rc2


def test_default_instances_renewed_and_data_carried():
    rc1 = make_rc_with_custom_stack()
    m1 = rc1.br_opt.rw_inst

    val_aliases_1 = find_aliases_by_type(m1, Values)
    var_alias_1 = pick_alias(m1, Variables, prefer="var")

    rc2 = rc1.get_renewed_self_instance()
    m2 = rc2.br_opt.rw_inst

    val_aliases_2 = find_aliases_by_type(m2, Values)
    assert len(val_aliases_2) >= 1
    var_alias_2 = pick_alias(m2, Variables, prefer=var_alias_1)

    for i, a1 in enumerate(val_aliases_1, start=1):
        assert a1 in m2, f"Missing Values alias {a1} in renewed map"
        v1, v2 = m1[a1], m2[a1]
        assert isinstance(v2, Values)
        assert v2 is not v1
        assert getattr(v2, f"a{i}") == (i, i + 1)

    v1, v2 = m1[var_alias_1], m2[var_alias_2]
    assert isinstance(v2, Variables)
    assert v2 is not v1
    assert v2.num == 42
    assert v2.obj is v1.obj


def test_op_stack_name_is_set_for_renewed_defaults():
    rc1 = make_rc_with_custom_stack()
    expected_stack = rc1.operation_stack
    rc2 = rc1.get_renewed_self_instance()
    m2 = rc2.br_opt.rw_inst

    for a in find_aliases_by_type(m2, Values):
        assert m2[a]._op_stack_name == expected_stack
    for a in find_aliases_by_type(m2, Variables):
        assert m2[a].__dict__.get("_op_stack_name") == expected_stack


def test_branch_options_fields_copied_in_last_only():
    rc1 = make_rc_with_custom_stack()

    rc1.br_opt.raise_err_cond = lambda x: False
    rc1.br_opt.distribute_input_data = True

    rc2 = rc1.get_renewed_self_instance()

    assert rc2.opt_stack[0] is rc1.opt_stack[0]

    old_last = rc1.opt_stack[-1]
    new_last = rc2.opt_stack[-1]

    assert new_last is not old_last
    assert new_last.br_name == old_last.br_name
    assert new_last.assign == old_last.assign
    assert new_last.force_call == old_last.force_call
    assert bool(new_last.distribute_input_data) == bool(
        old_last.distribute_input_data)
    assert new_last.raise_err_cond is old_last.raise_err_cond


def test_self_consistency_links():
    rc1 = make_rc_with_custom_stack()

    m1_before = rc1.br_opt.rw_inst
    run_conf1_before = m1_before.get("run_conf")

    rc2 = rc1.get_renewed_self_instance()

    m2 = rc2.br_opt.rw_inst
    assert m2["run_conf"] is rc2
    assert rc2.br_opt.rw_inst is m2

    assert rc1.br_opt.rw_inst is m1_before
    assert rc1.br_opt.rw_inst.get("run_conf") is run_conf1_before


def test_cfg_alias_preserved_by_reference():
    rc1 = make_rc_with_custom_stack()
    cfg_before = rc1.br_opt.rw_inst["cfg"]
    rc2 = rc1.get_renewed_self_instance()
    cfg_after = rc2.br_opt.rw_inst["cfg"]

    assert cfg_after is cfg_before
