from typing import Tuple

from src.branch_storm.operation import Operation as op, CallObject as obj
from src.branch_storm.branch import Branch as br
from src.branch_storm.type_containers import MandatoryArgTypeContainer as m


def ret_1_10_20_30() -> Tuple[int, int, int, int]:
    return 1, 10, 20, 30


def ret_1_10_20_30_40() -> Tuple[int, int, int, int, int]:
    return 1, 10, 20, 30, 40


def ret7() -> int:
    return 7


def ret8() -> int:
    return 8


def none_ret() -> None:
    return None


def echo_int(x: int) -> int:
    return x


def test_none_result_inside_distribution_is_collected_as_element():
    actual = br("root")[
        op(obj(none_ret)()).distribute_input_data,
        obj(ret7)(),
        op(obj(ret8)()).stop_distribution,
    ].run()

    assert actual == (None, 7, 8)


def test_auto_stop_when_delayed_return_len1_returns_scalar_not_1tuple():
    actual = br("root")[
        op(obj(ret7)()).distribute_input_data,
    ].run()

    assert actual == 7


def test_entrypoint_rem_args_with_internal_distribution_tail_is_preserved_for_parent_distribution():
    actual = br("root")[
        obj(ret_1_10_20_30)(),
        op(obj(echo_int)(m[int])).distribute_input_data,

        br("child")[
            op(obj(echo_int)(m[int])).distribute_input_data,
            obj(ret7)(),
            op(obj(ret8)()).stop_distribution,
        ],

        op(obj(echo_int)(m[int])),
        op(obj(echo_int)(m[int])).stop_distribution,
    ].run()

    assert actual == (1, 10, 7, 8, 20, 30)


def test_entrypoint_rem_args_with_end_chain_if_skips_mid_ops_but_tail_returns_unchanged():
    actual = br("root")[
        obj(ret_1_10_20_30)(),
        op(obj(echo_int)(m[int])).distribute_input_data,

        br("child")[
            op(obj(echo_int)(m[int])).distribute_input_data,

            op(obj(ret7)()).end_chain_if(lambda x: x in ((), None)),

            op(obj(ret8)()).force_call,
            op(obj(ret7)()).stop_distribution,
        ],

        op(obj(echo_int)(m[int])),
        op(obj(echo_int)(m[int])).stop_distribution,
    ].run()

    assert actual == (1, 10, 8, 7, 20, 30)


def test_entrypoint_rem_args_two_level_tails_when_first_atom_is_branch_do_not_mix():
    actual = br("root")[
        obj(ret_1_10_20_30_40)(),
        op(obj(echo_int)(m[int])).distribute_input_data,

        br("br1")[
            br("br2")[
                op(obj(echo_int)(m[int])).distribute_input_data,
                obj(ret7)(),
                op(obj(ret8)()).stop_distribution,
            ],
            op(obj(echo_int)(m[int])).distribute_input_data,
            obj(echo_int)(m[int]),
            op(obj(echo_int)(m[int])).stop_distribution,
        ],

        br("br3")[obj(echo_int)(m[int])],
        obj(echo_int)(m[int]),
        op(obj(echo_int)(m[int])).stop_distribution,
    ].run()

    assert actual == (1, 10, 7, 8, 20, 30, 40)
