import re
from typing import Tuple

import pytest

from src.branch_storm.launch_operations.errors import RemainingArgsFoundError
from src.branch_storm.operation import Operation as op, CallObject as obj
from src.branch_storm.branch import Branch as br
from src.branch_storm.type_containers import MandatoryArgTypeContainer as m


def ret_1_2_3() -> Tuple[int, int, int]: return 1, 2, 3
def ret_1_2() -> Tuple[int, int]: return 1, 2
def ret_1() -> int: return 1
def ret_2() -> int: return 2
def ret_3() -> int: return 3
def echo_int(x: int) -> int: return x
def pack3(a: int, b: int, c: int) -> Tuple[int, int, int]: return a, b, c


def test_distribution_two_windows_stop_and_restart():
    """
    Two distribution windows:
      - window1: f2..f4 produces delayed_return (flattened) -> (1,2,3)
      - window2: f5..f7 produces delayed_return (flattened) -> (1,2,3)
      - f8 consumes (1,2,3) as args
    """
    result = br("trusted_to_enriched")[
        op(obj(ret_1_2_3)()).op_name("seed"),
        op(obj(echo_int)(m[int])).distribute_input_data.op_name("w1_a"),
        op(obj(echo_int)(m[int])).op_name("w1_b"),
        op(obj(echo_int)(m[int])).stop_distribution.op_name("w1_stop"),
        op(obj(echo_int)(m[int])).distribute_input_data.op_name("w2_a"),
        op(obj(echo_int)(m[int])).op_name("w2_b"),
        op(obj(echo_int)(m[int])).stop_distribution.op_name("w2_stop"),
        op(obj(pack3)(m[int], m[int], m[int])).op_name("pack"),
    ].run()

    assert result == (1, 2, 3)


def test_distribution_mixed_ops_and_callobjects_equivalent():
    """
    Same as above but mixing op(...) and plain obj(...) (CallObject -> auto Operation in processor).
    """
    result = br("trusted_to_enriched")[
        obj(ret_1_2_3)(),
        op(obj(echo_int)(m[int])).distribute_input_data,
        obj(echo_int)(m[int]),
        op(obj(echo_int)(m[int])).stop_distribution,
        obj(pack3)(m[int], m[int], m[int]),
        obj(pack3)(m[int], m[int], m[int]),
        op(obj(echo_int)(m[int])).distribute_input_data,
        obj(echo_int)(m[int]),
        op(obj(echo_int)(m[int])).stop_distribution,
        obj(pack3)(m[int], m[int], m[int]),
    ].run()

    assert result == (1, 2, 3)


def test_distribution_auto_stop_at_branch_end():
    """
    If distribution is opened and branch ends, stop_distribution is implicit.
    """
    result = br("trusted_to_enriched")[
        obj(ret_1_2_3)(),
        op(obj(echo_int)(m[int])).distribute_input_data,
        obj(echo_int)(m[int]),
        obj(echo_int)(m[int]),
    ].run()

    assert result == (1, 2, 3)


def test_distribution_start_and_immediate_stop_single_atom():
    result = br("trusted_to_enriched")[
        op(obj(ret_1)()).distribute_input_data.stop_distribution
    ].run()

    assert result == 1


def test_assign_then_distribution_consumes_assigned_values():
    result = br("trusted_to_enriched")[
        op(obj(ret_1_2_3)()).assign("val.a", "val.b", "val.c"),
        op(obj(echo_int)(x=m("val.a")[int])).distribute_input_data,
        obj(echo_int)(x=m("val.b")[int]),
        op(obj(echo_int)(x=m("val.c")[int])).stop_distribution,
        obj(pack3)(m[int], m[int], m[int]),
    ].run()

    assert result == (1, 2, 3)


def test_assign_distribution_pipeline_all_assigned():
    """
    Assign (val.field1..3) under distribution and then consume them under distribution again.
    """
    result = br("trusted_to_enriched")[
        op(obj(ret_1)()).assign("val.field1").distribute_input_data,
        op(obj(ret_2)()).assign("val.field2"),
        op(obj(ret_3)()).assign("val.field3").stop_distribution.op_name("stop1"),
        op(obj(echo_int)(m("val.field1")[int])).distribute_input_data,
        obj(echo_int)(m("val.field2")[int]),
        op(obj(echo_int)(m("val.field3")[int])).stop_distribution,
    ].run()

    assert result == (1, 2, 3)


def test_remaining_args_error_when_not_consumed_no_distribution():
    """
    ret_1_2_3 returns (1,2,3) as args stream for the next atom.
    echo_int consumes only one int -> (2,3) remain and must raise.
    """
    with pytest.raises(
        RemainingArgsFoundError,
        match=re.escape(
            "Operation: trusted_to_enriched -> echo_int().\n"
            "After executing the operation, data was detected that was not involved\n"
            "in the initialization/call. Len 2; Their types: [<class 'int'>, <class 'int'>]"
        ),
    ):
        br("trusted_to_enriched")[
            obj(ret_1_2_3)(),
            obj(echo_int)(m[int]),
        ].run()


def test_branch_burn_rem_args_allows_unconsumed_input_stream():
    """
    Branch does NOT take_all_args, so its entrypoint consumes only one arg (int=1),
    the rest (4,5) remain at top level. burn_rem_args at the branch level should swallow them.
    """
    result = br("trusted_to_enriched")[
        op(obj(echo_int)(m[int])).distribute_input_data,
        obj(ret_2)(),
    ].burn_rem_args.run((1, 4, 5))

    assert result == (1, 2)


def test_branch_without_burn_rem_args_raises_on_unconsumed_input_stream():
    with pytest.raises(RemainingArgsFoundError):
        br("trusted_to_enriched")[
            op(obj(echo_int)(m[int])).distribute_input_data,
            obj(ret_2)(),
        ].run((1, 4, 5))


def test_top_level_distribute_flag_is_noop_but_valid():
    """
    distribute_input_data on the top-level branch has no external level to affect,
    but must not break execution. Internal distribution still works if opened by atoms.
    """
    result = br("trusted_to_enriched")[
        obj(ret_1_2_3)(),
        op(obj(echo_int)(m[int])).distribute_input_data,
        obj(echo_int)(m[int]),
        obj(echo_int)(m[int]),
    ].distribute_input_data.run()

    assert result == (1, 2, 3)


def test_distribution_from_take_all_args_branch_then_pack():
    """
    Outer branch is take_all_args so first atom can consume from provided stream (1,2,3).
    """
    result = br("trusted_to_enriched")[
        op(obj(echo_int)(m[int])).distribute_input_data,
        obj(echo_int)(m[int]),
        op(obj(echo_int)(m[int])).stop_distribution,
        obj(pack3)(m[int], m[int], m[int]),
    ].take_all_args.run((1, 2, 3))

    assert result == (1, 2, 3)


def test_distribution_to_branch_last_atom_raises_due_to_rem_args_left():
    """
    Scenario:
      - seed returns (1,2,3)
      - w1 consumes 1 and opens distribution -> delayed_return holds [1], stream becomes (2,3)
      - br2 (no internal distribution flags) takes only one arg (2) as entrypoint and returns 2
        leaving rem_args (3) at outer level
      - branch ends => implicit stop_distribution, result becomes (1,2) (flattened),
        but rem_args (3) remain at TOP => error
    """
    with pytest.raises(RemainingArgsFoundError):
        br("trusted_to_enriched")[
            obj(ret_1_2_3)(),
            op(obj(echo_int)(m[int])).distribute_input_data,
            br("br2")[
                obj(echo_int)(m[int]),
                obj(echo_int)(m[int]),
            ],
        ].run()


def test_distribution_to_branch_middle_atom_ok_when_followup_consumes_rest():
    """
    Same as above but after br2 we still have another consumer for the leftover arg.
    """
    result = br("trusted_to_enriched")[
        obj(ret_1_2_3)(),
        op(obj(echo_int)(m[int])).distribute_input_data,
        br("br2")[obj(echo_int)(m[int])],
        obj(echo_int)(m[int]),
    ].run()

    assert result == (1, 2, 3)


def test_distribution_to_nested_branches_take_all_args_ok():
    """
    Inner br2 is take_all_args, so it's a greedy entrypoint for the parent level,
    ensuring no rem_args are left at that parent boundary.
    """
    result = br("trusted_to_enriched")[
        obj(ret_1_2_3)(),
        br("br2")[
            op(obj(echo_int)(m[int])).distribute_input_data,
            br("br3")[
                obj(echo_int)(m[int]),
                obj(echo_int)(m[int]),
            ],
            obj(echo_int)(m[int]),
        ].take_all_args,
    ].run()

    assert result == (1, 2, 3)


def test_distribution_to_nested_branches_wo_take_all_args_neg():
    with pytest.raises(RemainingArgsFoundError):
        br("trusted_to_enriched")[
            obj(ret_1_2_3)(),
            br("br2")[
                op(obj(echo_int)(m[int])).distribute_input_data,
                obj(ret_1)()
            ], # (2, 3) should remain
        ].run()


def test_nested_take_all_args_must_be_greedy_even_if_parent_can_consume_leftovers():
    with pytest.raises(RemainingArgsFoundError):
        br("root")[
            obj(ret_1_2_3)(),
            br("br2")[obj(echo_int)(m[int])].take_all_args,
            obj(echo_int)(m[int]),
            obj(echo_int)(m[int])
        ].run()


def test_distribution_to_nested_branches_take_all_args_raises_on_extra_input():
    with pytest.raises(RemainingArgsFoundError):
        br("trusted_to_enriched")[
            op(obj(echo_int)(m[int])).distribute_input_data,
            br("br2")[
                obj(echo_int)(m[int]),
                br("br3")[obj(echo_int)(m[int])],
            ],
            obj(echo_int)(m[int]),
        ].take_all_args.run((1, 2, 3, 4))


def test_branch_open_distribution_as_atom_accumulates_and_passes_stream():
    """
    Branch br2 opens distribution on its *parent* level:
      - br2 executes, its result(s) go into delayed_return at parent level
      - its rem_args become the stream for next atoms on parent level

    br2: echo_int consumes 1 arg from incoming stream produced by br1 distribution window.
    br1 returns (1,2) (flattened). br2 consumes 1 -> outputs 1 and produces ret2() -> 2
    br3 consumes remaining 2 -> outputs 2 and ret3() -> 3
    Parent delayed_return => (1,2,2,3)
    """
    result = br("trusted_to_enriched")[
        br("br1")[
            op(obj(ret_1)()).distribute_input_data,
            obj(ret_2)(),
        ],
        br("br2")[
            op(obj(echo_int)(m[int])).distribute_input_data,
            obj(ret_2)(),
        ].distribute_input_data,
        br("br3")[
            op(obj(echo_int)(m[int])).distribute_input_data,
            obj(ret_3)(),
        ].stop_distribution,
    ].run()

    assert result == (1, 2, 2, 3)


def test_branch_to_branch_all_take_all_args():
    """
    br2.take_all_args makes it greedy entrypoint at parent level: it receives whole stream from br1.
    """
    result = br("trusted_to_enriched")[
        br("br1")[
            op(obj(ret_1)()).distribute_input_data,
            obj(ret_2)(),
            obj(ret_3)(),
        ],
        br("br2")[
            op(obj(echo_int)(m[int])).distribute_input_data,
            obj(echo_int)(m[int]),
            obj(echo_int)(m[int]),
        ].take_all_args,
    ].run()

    assert result == (1, 2, 3)


def test_distribution_branch_then_plain_ops():
    """
    br1 opens distribution on parent level; br2 (no take_all_args) consumes only one arg at entrypoint,
    leaving the rest to be consumed by plain operations after it.
    """
    result = br("trusted_to_enriched")[
        br("br1")[
            op(obj(ret_1)()).distribute_input_data,
            obj(ret_2)(),
            obj(ret_3)(),
        ].distribute_input_data,
        br("br2")[
            obj(echo_int)(m[int]),
            obj(echo_int)(m[int]),
            obj(echo_int)(m[int]),
        ],
        obj(echo_int)(m[int]),
        obj(echo_int)(m[int]),
    ].run()

    assert result == (1, 2, 3)


def test_entrypoint_first_atom_is_branch_consumes_one_arg_only():
    """
    First atom is br2 (not take_all_args). br2's entrypoint is its first op: echo_int(m[int]).
    So outer branch consumes only one arg (1) and leaves (2,3) as rem_args at top level -> error.
    """
    with pytest.raises(RemainingArgsFoundError):
        br("trusted_to_enriched")[
            br("br2")[
                obj(echo_int)(m[int]),
                obj(ret_1)(),
            ],
            obj(ret_1)(),
        ].take_all_args.run((1, 2, 3))


def test_not_open_but_stop_distribution_no_op():
    result = br("br")[
        op(obj(ret_1)()).stop_distribution.stop_distribution,
    ].stop_distribution.stop_distribution.run()

    assert result == 1


def test_double_open_distribution_no_op():
    result = br("br")[
        op(obj(ret_1)()).distribute_input_data.distribute_input_data,
    ].distribute_input_data.distribute_input_data.run()

    assert result == 1


def test_entrypoint_first_atom_is_branch_take_all_args_is_greedy_ok():
    """
    First atom is br2.take_all_args => greedy entrypoint visible to parent:
    parent must not keep rem_args at its level.
    """
    result = br("trusted_to_enriched")[
        br("br2")[
            op(obj(echo_int)(m[int])).distribute_input_data,
            obj(echo_int)(m[int]),
            obj(echo_int)(m[int]),
        ].take_all_args,
    ].take_all_args.run((1, 2, 3))

    assert result == (1, 2, 3)


def test_entrypoint_first_atom_is_branch_with_zero_arg_first_op_leaves_all_rem_args():
    """
    If entrypoint op consumes 0 args (ret1()), then parent branch consumes 0 and leaves ALL input as rem_args.
    Here top-level gets extra args -> error.
    """
    with pytest.raises(RemainingArgsFoundError):
        br("trusted_to_enriched")[
            br("br2")[
                obj(ret_1)(),  # consumes 0
                obj(ret_2)(),
            ],
        ].take_all_args.run((10, 20))


def test_distribution_to_branch_negative_exact_message():
    with pytest.raises(
        RemainingArgsFoundError,
        match=re.escape(
            "Operation: trusted_to_enriched -> last_op.\n"
            "After executing the operation, data was detected that was not "
            "involved\n"
            "in the initialization/call. Len 1; Their types: [<class 'int'>]\n"
        ),
    ):
        br("trusted_to_enriched")[
            br("br2")[
                obj(echo_int)(m[int]),
                br("br3")[obj(echo_int)(m[int])],
            ].distribute_input_data,
            obj(echo_int)(m[int]),
            op(obj(echo_int)(m[int])).op_name("last_op"),
        ].take_all_args.run((1, 2, 3, 4))


def test_assign_distribution_branch_to_branch():
    result = br("trusted_to_enriched")[
        br("br1")[
            op(obj(ret_1)()).assign("val.field1").distribute_input_data,
            op(obj(ret_2)()).assign("val.field2"),
            op(obj(ret_3)()).assign("val.field3"),
        ],
        br("br2")[
            op(obj(echo_int)(m("val.field1")[int])).distribute_input_data,
            obj(echo_int)(m("val.field2")[int]),
            obj(echo_int)(m("val.field3")[int]),
        ],
    ].run()

    assert result == (1, 2, 3)


def test_top_level_branch_distribute_flag_is_noop_and_does_not_change_behavior():
    """
    Without distribution, ret_2() receives input_data=1 (from ret_1()) and must fail:
    remaining args found.
    The SAME must happen even if top-level branch has distribute_input_data flag (no parent).
    """
    with pytest.raises(RemainingArgsFoundError):
        br("root")[
            obj(ret_1)(),
            obj(ret_2)(),
        ].run()

    # with top-level flag: must be NO-OP, still fails the same way
    with pytest.raises(RemainingArgsFoundError):
        br("root")[
            obj(ret_1)(),
            obj(ret_2)(),
        ].distribute_input_data.run()


def test_parent_distribution_does_not_leak_into_child_branch_internals():
    """
    If distribution is open at parent level, it must NOT automatically open distribution
    inside nested branches. So child branch executes sequentially and must fail:
    ret_2() receives input_data=1 and leaves rem_args.
    """
    with pytest.raises(
            RemainingArgsFoundError,
            match=re.escape(
                "Operation: root -> child -> ret_2().\n"
                "After executing the operation, data was detected that was not involved\n"
                "in the initialization/call. "
                "Len 1; Their types: [<class 'int'>]")):
        br("root")[
            op(obj(ret_3)()).distribute_input_data,
            br("child")[
                obj(ret_1)(),
                obj(ret_2)(),
            ],
        ].run()


def test_branch_atom_can_open_distribution_at_parent_level_and_collect_results():
    """
    Branch atom with .distribute_input_data opens distribution at parent level,
    subsequent atoms append to delayed_return,
    stop_distribution closes it,
    and next op consumes the returned flow.
    """
    actual = br("root")[
        br("b1")[obj(ret_1)()].distribute_input_data,
        obj(ret_2)(),
        op(obj(ret_3)()).stop_distribution,
        obj(pack3)(m[int], m[int], m[int]),
    ].run()

    assert actual == (1, 2, 3)


def test_operation_can_open_distribution_and_branch_atom_participates_like_operation():
    actual = br("root")[
        op(obj(ret_1)()).distribute_input_data,
        br("b1")[obj(ret_2)()],
        op(obj(ret_3)()).stop_distribution,
        obj(pack3)(m[int], m[int], m[int]),
    ].run()

    assert actual == (1, 2, 3)


def test_stop_distribution_on_branch_atom_closes_distribution():
    """
    stop_distribution on Branch atom should close distribution at current level
    exactly like stop_distribution on Operation.
    """
    actual = br("root")[
        op(obj(ret_1)()).distribute_input_data,
        br("b1")[obj(ret_2)()].stop_distribution,
    ].run()

    assert actual == (1, 2)


def test_branch_level_distribution_collects_branch_results_and_stops_at_end():
    actual = br("root")[
        br("b1")[obj(ret_1)()].distribute_input_data,
        br("b2")[obj(ret_2)()],
        br("b3")[obj(ret_3)()],
    ].run()

    assert actual == (1, 2, 3)


def test_branch_level_stop_distribution_then_new_distribution_can_start_again():
    actual = br("root")[
        br("b1")[obj(ret_1)()].distribute_input_data,
        br("b2")[obj(ret_2)()].stop_distribution,
        br("b3")[obj(ret_3)()].distribute_input_data.burn_rem_args,
    ].run()

    assert actual == 3


def test_distribution_through_branch_then_operation_consumes_remaining_flow():
    actual = br("root")[
        obj(ret_1_2_3)(),
        op(obj(echo_int)(m[int])).distribute_input_data,
        br("b1")[obj(echo_int)(m[int])],
        op(obj(echo_int)(m[int])).stop_distribution,
    ].run()

    assert actual == (1, 2, 3)


def test_branch_level_distribution_leftover_args_without_burn_or_next_distribution_raises():
    with pytest.raises(RemainingArgsFoundError):
        br("root")[
            obj(ret_1_2_3)(),
            op(obj(echo_int)(m[int])).distribute_input_data,
            br("b1")[obj(echo_int)(m[int])],
        ].run()


def test_take_all_args_plus_burn_remaining():
    with pytest.raises(
            RemainingArgsFoundError,
            match=re.escape(
                "Operation: root -> ret_1().\n"
                "After executing the operation, data was detected that was not involved\n"
                "in the initialization/call. "
                "Len 3; Their types: [<class 'int'>, <class 'int'>, <class 'int'>]")):
        br("root")[
            obj(ret_1)(),
        ].take_all_args.burn_rem_args.run((1, 2, 3))


def test_child_take_all_args():
    with pytest.raises(
            RemainingArgsFoundError,
            match=re.escape(
                "Operation: root -> child -> echo_int().\n"
                "After executing the operation, data was detected that was not involved\n"
                "in the initialization/call. "
                "Len 2; Their types: [<class 'int'>, <class 'int'>]")):
        br("root")[
            br("child")[
                obj(echo_int)(m[int]),
            ].take_all_args,
        ].run((1, 2, 3))
