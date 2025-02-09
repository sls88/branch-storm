import re
from typing import Tuple, List

import pytest

from src.operation_branch import Operation as op
from src.processor.initialization import GetPosArg, is_it_arg_type
from src.processor.one_op_processor import process_one_operation
from src.type_containers import ArgTypeContainer as at, SeqIdenticalTypesContainer as seqt


def return1() -> int:
    return 1


def return_args(arg1: int, arg2: int) -> Tuple[int, int]:
    return arg1, arg2


def return_empty_tuple() -> Tuple:
    return ()


def return_none():
    return None


def args_kwargs_func(arg1: List[None], arg2, arg3: int, arg4: str, arg5: str, arg6: int, *argss,
                     kwarg1: int, kwarg2: str = "7i", kwarg3: str, **kwargss):
    return arg1, arg2, arg3, arg4, arg5, arg6, argss, kwarg1, kwarg2, kwarg3, kwargss


def test_process_one_op_func_without_operation_shell():
    actual_result = process_one_operation(return1, (), {})

    assert actual_result == (1, 'return1', None)


def test_process_one_op_func_without_args_wo_func():
    actual_result = process_one_operation(op(return1), (), {})

    assert actual_result == (1, 'return1', None)


def test_process_one_op_func_without_args_with_func():
    actual_result = process_one_operation(op(return1).func(), (), {})

    assert actual_result == (1, 'return1', None)



def test_process_one_op_func_with_two_args_one_remain():
    operation = op(return_args).func(at[int], at[int])
    actual_result = process_one_operation(operation, (1, 2, 3), {})

    assert actual_result == ((1, 2), 'return_args', (3,))


def test_process_one_op_func_with_two_args():
    operation = op(return_args).func(2, 3)
    actual_result = process_one_operation(operation, (1,), {}, "trusted_to_enriched", "dim_r")

    assert actual_result == ((2, 3), 'trusted_to_enriched -> dim_r', (1,))


def test_process_one_op_func_with_two_kwargs():
    operation = op(return_args).func(arg1=2, arg2=3)
    actual_result = process_one_operation(operation, (1,), {}, op_name="dim_r")

    assert actual_result == ((2, 3), 'dim_r', (1,))


def test_process_one_op_func_return_empty_tuple():
    actual_result = process_one_operation(op(return_empty_tuple), (), {}, "trusted_to_enriched")

    assert actual_result == ((), 'trusted_to_enriched -> return_empty_tuple', None)


def test_process_one_op_func_return_none():
    actual_result = process_one_operation(op(return_none), (), {})

    assert actual_result == (None, 'return_none', None)


def test_process_one_op_big_function():
    operation = op(args_kwargs_func).func(
        [None], 5, at[int], "uuu", at[str], 3,
        at[float, seqt[bool], str, str, seqt[int]], "x5", at[str],
        kwarg1=9, kwarg3=GetPosArg(at[str], 3), kw100=90,
        kw200=GetPosArg(at[str], 4))

    init_data = (1, "tt", "lll", "pppp", 4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F", 13)
    actual_result = process_one_operation(operation, init_data, {})
    expected_data = (
        [None], 5, 1, 'uuu', 'tt', 3,
        (4.0, True, True, True, 'str1', 'str2', 1, 2, 3, 4, 'x5', 'F'),
        9, '7i', 'lll', {'kw100': 90, 'kw200': 'pppp'})

    assert actual_result == (expected_data, 'args_kwargs_func', (13,))


def test_is_it_arg_type():
    assert (is_it_arg_type(at), is_it_arg_type(at[str])) == (True, True)


def test_process_one_op_function_with_type_stubs():
    operation = op(args_kwargs_func).func(
        [None], 5, at, "uuu", at, 3, at, "x5",
        kwarg1=9, kwarg3=GetPosArg(at, 3), kw100=90,
        kw200=GetPosArg(at, 4))

    init_data = (1, "tt", "lll", "pppp", 4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F", 13)
    actual_result = process_one_operation(operation, init_data, {})
    expected_data = (
        [None], 5, 1, 'uuu', 'tt', 3,
        (4.0, True, True, True, 'str1', 'str2', 1, 2, 3, 4, 'F', 13, 'x5'),
        9, '7i', 'lll', {'kw100': 90, 'kw200': 'pppp'})

    assert actual_result == (expected_data, 'args_kwargs_func', None)


def test_process_one_op_function_with_type_stubs_get_arg_after_receiving_all_in_args():
    operation = op(args_kwargs_func).func(
        [None], 5, at, "uuu", at, 3, at,
        kwarg1=9, kwarg3=GetPosArg(at, 3), kw100=90,
        kw200=GetPosArg(at, 4))

    init_data = (1, "tt", "lll", "pppp", 4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F", 13)
    actual_result = process_one_operation(
        operation, init_data, {}, "trusted_to_enriched")
    expected_data = ([None], 5, 1, 'uuu', 'tt', 3,
                     (4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F", 13),
                     9, '7i', 'lll',
                     {'kw100': 90, 'kw200': 'pppp'})

    assert actual_result == (expected_data, 'trusted_to_enriched -> args_kwargs_func', None)


def test_process_one_op_function_check_type_fail_neg():
    operation = op(return_args).func(at[str], arg2=GetPosArg(at[int], 2))
    init_data = (1, 2.0)
    with pytest.raises(
            TypeError,
            match=re.escape(
                "Len: 2; Arg type map: {'arg1': (<class 'int'>, <class 'str'>), "
                "'arg2': (<class 'float'>, <class 'int'>)}")):
        process_one_operation(operation, init_data, {})


def test_process_one_op_function_with_type_wo_type_in_args():
    operation = op(args_kwargs_func).func(
        [None], 5, at, "uuu", at, 3, "x5", at[float], at[bool],
        kwarg1=9, kwarg3=GetPosArg(at, 3), kw100=90,
        kw200=GetPosArg(at, 4))

    init_data = (1, "tt", "lll", "pppp", 4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F", 13)
    actual_result = process_one_operation(
        operation, init_data, {}, "trusted_to_enriched")
    expected_data = ([None], 5, 1, 'uuu', 'tt', 3, ('x5', 4.0, True), 9, '7i', 'lll',
                     {'kw100': 90, 'kw200': 'pppp'})

    assert actual_result == (expected_data, 'trusted_to_enriched -> args_kwargs_func',
                             (True, True, 'str1', 'str2', 1, 2, 3, 4, 'F', 13))


def test_process_one_op_function_with_type_with_one_type_in_args_and_further_seq():
    operation = op(args_kwargs_func).func(
        [None], 5, at, "uuu", at, 3, at[float], seqt[int], "O",
        kwarg1=9, kwarg3=GetPosArg(at, 3), kw100=90,
        kw200=GetPosArg(at, 4))

    init_data = (1, "tt", "lll", "pppp", 4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F", 13)
    actual_result = process_one_operation(
        operation, init_data, {}, "trusted_to_enriched")
    expected_data = ([None], 5, 1, 'uuu', 'tt', 3,
                     (4.0, True, True, True, "O"),
                     9, '7i', 'lll',
                     {'kw100': 90, 'kw200': 'pppp'})

    assert actual_result == (expected_data, 'trusted_to_enriched -> args_kwargs_func',
                             ('str1', 'str2', 1, 2, 3, 4, 'F', 13))


def test_process_one_op_function_with_type_stubs_wo_stub_on_args():
    operation = op(args_kwargs_func).func(
        [None], 5, at, "uuu", at, 3, "x5", 7, "x6",
        kwarg1=9, kwarg3=GetPosArg(at, 3), kw100=90,
        kw200=GetPosArg(at, 4))

    init_data = (1, "tt", "lll", "pppp", 4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F", 13)
    actual_result = process_one_operation(
        operation, init_data, {}, "trusted_to_enriched")
    expected_data = ([None], 5, 1, 'uuu', 'tt', 3, ('x5', 7, 'x6'), 9, '7i', 'lll',
                     {'kw100': 90, 'kw200': 'pppp'})

    assert actual_result == (expected_data, 'trusted_to_enriched -> args_kwargs_func',
                             (4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F", 13))


def test_process_one_op_not_enough_input_data_positions_neg():
    operation = op(args_kwargs_func).func(
        [None], 5, at[int], "uuu", at[str], 3,
        at[float, seqt[bool], str, str, seqt[int]], "x5", at[str],
        kwarg1=9, kwarg3=GetPosArg(at[str], 3), kw100=90,
        kw200=GetPosArg(at[str], 4))

    init_data = (1, "tt", "lll")
    with pytest.raises(
            TypeError,
            match=re.escape("{1: <class 'float'>, 2: <class 'bool'>, "
                            "3: <class 'str'>, 4: <class 'str'>, "
                            "5: <class 'int'>, 7: <class 'str'>, "
                            "'kw200': <class 'str'>}")):
        process_one_operation(operation, init_data, {})


def def_args(a, b=1, c=2, *args) -> Tuple:
    return a, b, c, args


def test_process_one_op_function_def_args_expected_int():
    operation = op(def_args).func(100, 200, at[int])

    init_data = (1,)
    actual_result = process_one_operation(
        operation, init_data, {}, "trusted_to_enriched", "def_func")
    expected_data = (100, 200, 1, ())

    assert actual_result == (expected_data, 'trusted_to_enriched -> def_func', None)


def test_process_one_op_function_def_args_expected_int_and_int_in_args():
    operation = op(def_args).func(100, 200, at[int], at[int])

    init_data = (10, 20, "str")
    actual_result = process_one_operation(
        operation, init_data, {}, "trusted_to_enriched")
    expected_data = (100, 200, 10, (20,))

    assert actual_result == (expected_data, 'trusted_to_enriched -> def_args', ("str",))


def test_process_one_op_function_incorrect_containers_neg():
    operation = op(def_args).func(seqt[int], at[int, float])

    init_data = ()
    with pytest.raises(
            TypeError,
            match=re.escape(
                "Len: 2; Incorrect_containers: "
                "{'a': 'sequence_for_one_arg', 'b': (<class 'int'>, <class 'float'>)}")):
        process_one_operation(operation, init_data, {})


def test_process_one_op_function_def_args_expected_int_and_stub_in_args():
    operation = op(def_args).func(100, 200, at[int], at)

    init_data = (10, 20, "str", True, 1.1, [5], {}, ())
    actual_result = process_one_operation(
        operation, init_data, {}, "trusted_to_enriched")
    expected_data = (100, 200, 10, (20, "str", True, 1.1, [5], {}, ()))

    assert actual_result == (expected_data, 'trusted_to_enriched -> def_args', None)


def test_process_one_op_function_def_args_data_not_expected():
    operation = op(def_args).func(100, 200)

    init_data = (1,)
    actual_result = process_one_operation(
        operation, init_data, {}, "trusted_to_enriched", "def_func")
    expected_data = (100, 200, 2, ())

    assert actual_result == (expected_data, 'trusted_to_enriched -> def_func', (1,))
