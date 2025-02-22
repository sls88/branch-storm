import re
from typing import Tuple, List

import pytest

from src.operation import Operation as op, CallObject as obj
from src.processor.initialization import GetPosArg, is_it_arg_type
from src.type_containers import MandatoryArgTypeContainer as mat, OptionalArgTypeContainer as oat, \
    SeqIdenticalTypesContainer as seqt


def return1() -> int:
    return 1


def return_args(arg1: int, arg2: int) -> Tuple[int, int]:
    return arg1, arg2


def return_empty_tuple() -> Tuple:
    return ()


def return_none():
    return None


def receive_seq_args(*args):
    return args


def args_kwargs_func(arg1: List[None], arg2, arg3: int, arg4: str, arg5: str, arg6: int, *argss,
                     kwarg1: int, kwarg2: str = "7i", kwarg3: str, **kwargss):
    return arg1, arg2, arg3, arg4, arg5, arg6, argss, kwarg1, kwarg2, kwarg3, kwargss



def test_process_one_op_func_without_args():
    actual_result = op(obj(return1)()).run(())

    assert actual_result == (1, None)


def test_process_one_op_func_with_two_args_one_remain():
    actual_result = op(obj(return_args)(mat[int], mat[int])).run((1, 2, 3))

    assert actual_result == ((1, 2), (3,))


def test_process_one_op_func_with_two_args():
    operation = op(obj(return_args)(2, 3)).op_name("dim_r")
    operation._set_branch_stack("stack")
    actual_result = operation.run((1,))
    actual_op_stack = operation._operation_stack

    assert actual_result == ((2, 3), (1,))
    assert actual_op_stack == 'stack -> dim_r'


def test_process_one_op_func_with_two_kwargs():
    operation = op(obj(return_args)(arg1=2, arg2=3))
    operation._set_branch_stack("stack")
    actual_result = operation.run((1,))
    actual_op_stack = operation._operation_stack

    assert actual_result == ((2, 3), (1,))
    assert actual_op_stack == 'stack -> return_args'



def test_process_one_op_func_return_empty_tuple():
    operation = op(obj(return_empty_tuple)())
    actual_result = operation.run(())
    actual_op_stack = operation._operation_stack

    assert actual_result == ((), None)
    assert actual_op_stack == "return_empty_tuple"


def test_process_one_op_func_return_none():
    actual_result = op(obj(return_none)()).run(())

    assert actual_result == (None, None)


def test_process_one_op_big_function():
    operation = op(obj(args_kwargs_func)(
        [None], 5, mat[int], "uuu", mat[str], 3,
        mat[float, seqt[bool], str, str, seqt[int]], "x5", mat[str],
        kwarg1=9, kwarg3=GetPosArg(mat[str], 3), kw100=90,
        kw200=GetPosArg(mat[str], 4)))

    init_data = (1, "tt", "lll", "pppp", 4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F", 13)
    actual_result = operation.run(init_data)
    expected_data = (
        [None], 5, 1, 'uuu', 'tt', 3,
        (4.0, True, True, True, 'str1', 'str2', 1, 2, 3, 4, 'x5', 'F'),
        9, '7i', 'lll', {'kw100': 90, 'kw200': 'pppp'})

    assert actual_result == (expected_data, (13,))


def test_is_it_arg_type():
    assert (is_it_arg_type(mat), is_it_arg_type(mat[str]),
            is_it_arg_type(oat), is_it_arg_type(oat[str])) == ('mandatory', 'mandatory', 'optional', 'optional')


def test_process_one_op_function_with_type_stubs():
    operation = op(obj(args_kwargs_func)(
        [None], 5, mat, "uuu", mat, 3, mat, "x5",
        kwarg1=9, kwarg3=GetPosArg(mat, 3), kw100=90,
        kw200=GetPosArg(mat, 4)))

    init_data = (1, "tt", "lll", "pppp", 4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F", 13)
    actual_result = operation.run(init_data)
    expected_data = (
        [None], 5, 1, 'uuu', 'tt', 3,
        (4.0, True, True, True, 'str1', 'str2', 1, 2, 3, 4, 'F', 13, 'x5'),
        9, '7i', 'lll', {'kw100': 90, 'kw200': 'pppp'})

    assert actual_result == (expected_data, None)


def test_process_one_op_function_with_type_stubs_get_arg_after_receiving_all_in_args():
    operation = op(obj(args_kwargs_func)(
        [None], 5, mat, "uuu", mat, 3, mat,
        kwarg1=9, kwarg3=GetPosArg(mat, 3), kw100=90,
        kw200=GetPosArg(mat, 4)))

    init_data = (1, "tt", "lll", "pppp", 4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F", 13)
    actual_result = operation.run(init_data)
    expected_data = ([None], 5, 1, 'uuu', 'tt', 3,
                     (4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F", 13),
                     9, '7i', 'lll',
                     {'kw100': 90, 'kw200': 'pppp'})

    assert actual_result == (expected_data, None)


def test_process_one_op_function_check_type_fail_neg():
    operation = op(obj(return_args)(mat[str], arg2=GetPosArg(mat[int], 2)))
    init_data = (1, 2.0)
    with pytest.raises(
            TypeError,
            match=re.escape(
                "Len: 2; Arg type map: {'arg1': (<class 'int'>, <class 'str'>), "
                "'arg2': (<class 'float'>, <class 'int'>)}")):
            operation.run(init_data)


def test_process_one_op_function_with_type_wo_type_in_args():
    operation = op(obj(args_kwargs_func)(
        [None], 5, mat, "uuu", mat, 3, "x5", mat[float], mat[bool],
        kwarg1=9, kwarg3=GetPosArg(mat, 3), kw100=90,
        kw200=GetPosArg(mat, 4)))

    init_data = (1, "tt", "lll", "pppp", 4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F", 13)
    actual_result = operation.run(init_data)
    expected_data = ([None], 5, 1, 'uuu', 'tt', 3, ('x5', 4.0, True), 9, '7i', 'lll',
                     {'kw100': 90, 'kw200': 'pppp'})

    assert actual_result == (
        expected_data, (True, True, 'str1', 'str2', 1, 2, 3, 4, 'F', 13))


def test_process_one_op_function_with_type_with_one_type_in_args_and_further_seq():
    operation = op(obj(args_kwargs_func)(
        [None], 5, mat, "uuu", mat, 3, mat[float], seqt[int], "O",
        kwarg1=9, kwarg3=GetPosArg(mat, 3), kw100=90,
        kw200=GetPosArg(mat, 4)))

    init_data = (1, "tt", "lll", "pppp", 4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F", 13)
    actual_result = operation.run(init_data)
    expected_data = ([None], 5, 1, 'uuu', 'tt', 3,
                     (4.0, True, True, True, "O"),
                     9, '7i', 'lll',
                     {'kw100': 90, 'kw200': 'pppp'})

    assert actual_result == (
        expected_data, ('str1', 'str2', 1, 2, 3, 4, 'F', 13))


def test_process_one_op_function_with_type_stubs_wo_stub_on_args():
    operation = op(obj(args_kwargs_func)(
        [None], 5, mat, "uuu", mat, 3, "x5", 7, "x6",
        kwarg1=9, kwarg3=GetPosArg(mat, 3), kw100=90,
        kw200=GetPosArg(mat, 4)))

    init_data = (1, "tt", "lll", "pppp", 4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F", 13)
    actual_result = operation.run(init_data)
    expected_data = ([None], 5, 1, 'uuu', 'tt', 3, ('x5', 7, 'x6'), 9, '7i', 'lll',
                     {'kw100': 90, 'kw200': 'pppp'})

    assert actual_result == (
        expected_data, (4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F", 13))


def test_process_one_op_not_enough_input_data_positions_neg():
    operation = op(obj(args_kwargs_func)(
        [None], 5, mat[int], "uuu", mat[str], 3,
        mat[float, seqt[bool], str, str, seqt[int]], "x5", mat[str],
        kwarg1=9, kwarg3=GetPosArg(mat[str], 3), kw100=90,
        kw200=GetPosArg(mat[str], 4)))

    init_data = (1, "tt", "lll")
    with pytest.raises(TypeError, match=re.escape(
            "Len: 6, Args map: {1: 'mandatory', 2: 'mandatory', "
            "3: 'mandatory', 4: 'mandatory', 5: 'mandatory', 7: 'mandatory'}")):
            operation.run(init_data)


def def_args(a, b=1, c=2, *args) -> Tuple:
    return a, b, c, args


def test_process_one_op_function_def_args_expected_int():
    operation = op(obj(def_args)(100, 200, mat[int]))

    init_data = (1,)
    actual_result = operation.run(init_data)
    expected_data = (100, 200, 1, ())

    assert actual_result == (expected_data, None)


def test_process_one_op_function_def_args_expected_int_and_int_in_args():
    operation = op(obj(def_args)(100, 200, mat[int], mat[int]))

    init_data = (10, 20, "str")
    actual_result = operation.run(init_data)
    expected_data = (100, 200, 10, (20,))

    assert actual_result == (expected_data, ("str",))


def test_process_one_op_function_incorrect_containers_neg():
    operation = op(obj(def_args)(seqt[int], mat[int, float]))

    init_data = ()
    with pytest.raises(TypeError, match=re.escape(
            "Len: 2; Incorrect type containers: "
            "{'a': 'sequence_for_one_arg', 'b': (<class 'int'>, <class 'float'>)}")):
        operation.run(init_data)


def test_process_one_op_function_def_args_expected_int_and_optional_rest():
    operation = op(obj(def_args)(100, mat[int], oat[int], oat[int], oat[int]))

    init_data = (20,)
    actual_result = operation.run(init_data)
    expected_data = (100, 20, 2, ())

    assert actual_result == (expected_data, None)


def test_process_one_op_function_def_args_expected_int_and_not_mandatory_neg():
    operation = op(obj(def_args)(100, mat[int], mat[int], mat[int], oat[int]))

    init_data = (20,)
    with pytest.raises(TypeError, match=re.escape(
            "Len: 2, Args map: {'c': 'mandatory', 1: 'mandatory'}")):
        operation.run(init_data)


def test_process_one_op_function_check_mand_after_opt_at_container_neg():
    operation = op(obj(def_args)(100, oat[int], mat[int], mat[int], oat[int], mat[int]))

    init_data = (20,)
    with pytest.raises(TypeError, match=re.escape(
            "Len 3, Args map: {'c': 'mandatory', 1: 'mandatory', 3: 'mandatory'}")):
        operation.run(init_data)


def test_process_one_op_function_def_args_data_not_expected():
    operation = op(obj(def_args)(100, 200))

    init_data = (1,)
    actual_result = operation.run(init_data)
    expected_data = (100, 200, 2, ())

    assert actual_result == (expected_data, (1,))


def test_process_one_op_function_receive_seq_args():
    operation = op(obj(receive_seq_args)(seqt[int], "100i", "200i"))

    init_data = (1, 2, 3)
    actual_result = operation.run(init_data)

    assert actual_result == ((1, 2, 3, "100i", "200i"), None)
