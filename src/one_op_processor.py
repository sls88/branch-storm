import uuid
from dataclasses import dataclass
from inspect import signature, Parameter, getcallargs
from itertools import islice
from typing import Any, Dict, List, Optional, Tuple, Union, get_args, Callable, Type, OrderedDict, \
    TypeVar

from typing_extensions import TypeVarTuple, Unpack, Generic


def get_params_wo_self(func: Callable, remove_first: bool = True) -> OrderedDict[str, Parameter]:
    """Parse function or method TypeHints and return metadata:

    remove_first = True
    class Op:
        def method(self,                              OrderedDict([
                   arg1: int,                             ('arg1', <Parameter "arg1: int">),
                   arg2: JobArgs,                   ->    ('arg2', <Parameter "arg2: JobArgs">),
                   arg3: Union[SomeClass, Transit],       ('arg3', <Parameter "arg3: Union[SomeClass, Transit]">),
                   arg4,                                  ('arg4', <Parameter "arg4">),
                   *args,                                 ('args', <Parameter "*args: str">),
                   **kwargs):                             ('kwargs', <Parameter "**kwargs">)
            pass                                      ])

    remove_first = False                             OrderedDict([
    def function(arg1: int,                              ('arg1', <Parameter "arg1: int">),
                 arg2: JobArgs,                          ('arg2', <Parameter "arg2: JobArgs">),
                 arg3: Union[SomeClass, Transit]  ->     ('arg3', <Parameter "arg3: Union[SomeClass, Transit]">),
                 arg4):                                  ('arg4', <Parameter "arg4">)
        pass                                         ])
    """
    parameters = signature(func).parameters
    if remove_first:
        param = parameters.copy()
        param.pop(list(param)[0])
        return param
    return parameters.copy()


@dataclass
class Param:
    par: Parameter
    par_kind: str = None
    # par_types: List[Type] = None
    par_default: Type = None

    def __post_init__(self):
        self.par_kind = self.par.kind.name
        self.par_default = self.par.default
        # classes_in_argtype = []
        # if get_origin(self.par.annotation) == Union:
        #     for one_obj in get_args(self.par.annotation):
        #         classes_in_argtype.append(one_obj)
        # else:
        #     classes_in_argtype.append(self.par.annotation)
        # self.par_types = classes_in_argtype

TT = TypeVarTuple('TT')
T = TypeVar('T')

class MandatoryArgTypeContainer(Generic[Unpack[TT]]):
    pass

class OptionalArgTypeContainer(Generic[Unpack[TT]]):
    pass

class SeqIdenticalTypesContainer(Generic[T]):
    pass

([None], "arg1", 1, "arg2")
("*args", [None], 1)
([None], 1, "*args")
([None], "*args", 1)

a = MandatoryArgTypeContainer[str, Union[List[str], bool], SeqIdenticalTypesContainer]
print(get_args(a))


def is_it_mand_arg_type(arg: Any) -> bool:
    if "__dict__" in dir(arg) and "__origin__" in arg.__dict__ and arg.__dict__["__origin__"] is MandatoryArgTypeContainer:
        return True
    return False


def is_it_opt_arg_type(arg: Any) -> bool:
    if "__dict__" in dir(arg) and "__origin__" in arg.__dict__ and arg.__dict__["__origin__"] is OptionalArgTypeContainer:
        return True
    return False


def is_it_seq_ident_types(arg: Any) -> bool:
    if "__dict__" in dir(arg) and "__origin__" in arg.__dict__ and arg.__dict__["__origin__"] is SeqIdenticalTypesContainer:
        return True
    return False


def get_args_from_arg_type(
        type_container: Union[Type[MandatoryArgTypeContainer], Type[OptionalArgTypeContainer]]) -> Tuple:
    return type_container.__dict__['__args__']


class PosArg:
    def __init__(self, arg_type: Type[MandatoryArgTypeContainer], number_position: int) -> None:
        self.arg_type = arg_type
        self.number_position = number_position
        if not is_it_mand_arg_type(arg_type):
            raise TypeError
        if len(get_args_from_arg_type(arg_type)) > 1:
            raise TypeError


parsed_arguments = ([None], MandatoryArgTypeContainer[str], 1, MandatoryArgTypeContainer[int], {"kwarg1": 1, "kwarg2": "2"})
def get_args_for_initialization(parsed_arguments, obj: Union[Callable, Type], *input_data, check_type: bool = True):
    params_wo_self = {name: par for name, par in get_params_wo_self(obj, False).items()}
    # print(params_wo_self)
    args, kwargs = [], {}

    # for arg in parsed_arguments:
    #     if is_it_mand_arg_type(arg):
    #         print(True)
    #     else:
    #         print(False)

# print(get_args_for_initialization(parsed_arguments, some_func, "u", 5))
# print(MandatoryArgType[str].__dict__['__origin__'] is MandatoryArgType)
# print(MandatoryArgType[str, Union[List[str], bool], EllipsisType].__dict__)
# print(dir(MandatoryArgType[str]))
def replace_kwargs_to_args(
        input_data: Tuple,
        kwargs: Dict[str, Any]) -> Tuple[Tuple, Dict[str, Any]]:
    input_data, new_kwargs = list(input_data), {}
    uniq_uuid = str(uuid.uuid4())
    for name, arg in kwargs.items():
        if not isinstance(arg, PosArg):
            new_kwargs[name] = arg
        else:
            #TODO check type data_arg
            new_kwargs[name] = input_data[arg.number_position - 1]
            input_data[arg.number_position - 1] = uniq_uuid
    return tuple(list(filter(lambda x: x != uniq_uuid, input_data))), new_kwargs


def move_init_d_args_to_kwargs(
        input_data: Tuple,
        kwargs: Dict[str, Any],
        var_keyword: Dict[str, Any]) -> Tuple[Tuple, Dict[str, Any], Dict[str, Any]]:
    input_data, kwargs = replace_kwargs_to_args(input_data, kwargs)
    input_data, var_keyword = replace_kwargs_to_args(input_data, var_keyword)
    return input_data, kwargs, var_keyword


def get_kwargs_var_keyword(
        input_data: Tuple,
        kwargs: Dict[str, Any],
        call_args: Dict[str, Any],
        params_kind: Dict[str, str]) -> Tuple[Tuple, Dict[str, Any], Dict[str, Any]]:
    var_keyword = {name: value for name, value in call_args.items() if params_kind.get(name) == "VAR_KEYWORD"}
    return move_init_d_args_to_kwargs(input_data, kwargs, var_keyword)


def mand_after_opt_cont_check(args: Tuple) -> None:
    opt_cont_flag = False
    for arg in args:
        if is_it_opt_arg_type(arg):
            opt_cont_flag = True
        elif is_it_mand_arg_type(arg) and opt_cont_flag:
            raise TypeError("Cannot receive mandatory argument after optional.")


def get_var_positional(args, input_data: Tuple):
    print(111, args)
    mand_after_opt_cont_check(args)
    new_args = ()
    for arg in args:
        if not is_it_mand_arg_type(arg) or not is_it_opt_arg_type(arg):
            new_args = (*new_args, arg)
        elif is_it_mand_arg_type(arg):
            arg_types = get_args_from_arg_type(arg)
            for arg_type in arg_types:
                if not is_it_seq_ident_types(arg):
                    #TODO check type raise err if not arg or not type fit arg_type
                    try:
                        new_args = (*new_args, input_data[0])
                        input_data = input_data[1:]
                    except IndexError:
                        raise ValueError("There is no arg")

    print(222, input_data)
    print(333, new_args)

# def separate_kwargs()

class Operation:
    def __init__(self, class_or_func, instance: Optional[Any] = None, method: Optional[str] = None):
        self.class_or_func = class_or_func
        self.instance = instance
        self.method = method

    def func(self, *args, **kwargs):
        input_data = (1, "tt", "lll", "pppp", 4, True, True, True, "str1", "str2")
        call_args = getcallargs(self.class_or_func, *args, **kwargs)
        print("KWARGS: ", kwargs)
        mand_after_opt_cont_check(args)
        args = dict(islice(call_args.items(), len(args) - 1))
        print("call_args", call_args)
        params_wo_self = {name: par for name, par in get_params_wo_self(self.class_or_func, False).items()}
        params_kind = {name: par.kind.name for name, par in params_wo_self.items()}
        print("params_kind", params_kind)
        # print("ARGS: ", args)
        # print("KWARGS: ", kwargs)
        # print(params_wo_self)
        input_data, kwargs, var_keyword = get_kwargs_var_keyword(input_data, kwargs, call_args, params_kind)
        print("id_kw_var_kw: ", input_data, kwargs, var_keyword)
        new_args = []
        for name, arg in call_args.items():
            param = params_wo_self[name]
            if is_it_seq_ident_types(arg) and param.kind.name != "VAR_POSITIONAL":
                raise TypeError("You cannot pass types sequence for one argument position.")
            elif (is_it_mand_arg_type(arg) or is_it_opt_arg_type(arg)) and param.kind.name != "VAR_POSITIONAL" \
                    and len(get_args_from_arg_type(arg)) > 1:
                raise TypeError("You cannot pass more than one type for one argument position.")
            elif not is_it_mand_arg_type(arg) and not is_it_opt_arg_type(arg) and \
                    param.kind.name in ["POSITIONAL_ONLY", "POSITIONAL_OR_KEYWORD"] and \
                    not param.kind.name == "VAR_POSITIONAL":
                new_args.append(arg)
            elif is_it_mand_arg_type(arg) and not is_it_opt_arg_type(arg) and \
                    param.kind.name in ["POSITIONAL_ONLY", "POSITIONAL_OR_KEYWORD"] and \
                    not param.kind.name == "VAR_POSITIONAL":
                arg_type = get_args_from_arg_type(arg)[0]
                #TODO check type raise err if not arg or not type fit
                try:
                    new_args.append(input_data[0])
                    input_data = input_data[1:]
                except IndexError:
                    raise ValueError("There is no arg")
            elif not is_it_mand_arg_type(arg) and is_it_opt_arg_type(arg) and \
                    param.kind.name in ["POSITIONAL_ONLY", "POSITIONAL_OR_KEYWORD"] and \
                    not param.kind.name == "VAR_POSITIONAL":
                arg_type = get_args_from_arg_type(arg)[0]
                #TODO check type raise err if arg exist and type not fit
                try:
                    new_args.append(input_data[0])
                    input_data = input_data[1:]
                except IndexError:
                    pass
            elif param.kind.name == "VAR_POSITIONAL":
                get_var_positional(arg, input_data)

        print(new_args, kwargs)
        print("id_input_data: ", input_data)
        return self

# op = Operation(some_func).func([None], MandatoryArgTypeContainer[str], 1, MandatoryArgTypeContainer[int], kwarg1=1, kwarg2=2)
# print(op.func_call_args)

# Parameter.
# print(11111111)
# print(MandatoryArgTypeContainer[str].__dict__['__args__'])
# print(SeqIdenticalTypesContainer[str].__dict__["__args__"])
# print(SeqIdenticalTypesContainer[str].__dict__["__origin__"])


input_data = (1, "tt", "lll", "pppp", 4, True, True, True, "str1", "str2")
def func(arg1: List[None], arg2, arg3: int, arg4: str, arg5: str, arg6: int, *argss,
         kwarg1: int, kwarg2: str = "7i", kwarg3: str, **kwargss):
    pass
op = Operation(func).func(
    [None], 5, MandatoryArgTypeContainer[int], "uuu",
    MandatoryArgTypeContainer[str], 3,
    MandatoryArgTypeContainer[int, SeqIdenticalTypesContainer[bool], str, str], 5,
    kwarg1=9, kwarg3=PosArg(MandatoryArgTypeContainer[str], 3), kw100=90,
    kw200=PosArg(MandatoryArgTypeContainer[str], 4))

actual_result = (([None], 5, 1, "uuu", "tt", 3, 4, True, True, True, "str1", "str2", 5),         #args
                 {"kwarg1": 9, "kwarg2": "7i", "kwarg3": "lll", "kw100": 90, "kw200": "pppp"})   #kwargs


# input_data = (1, "tt", "lll", 4)
# def func_opt(arg1: List[None], arg2, arg3: int, arg4: str, arg5: str, arg6: int, arg7: str = "opt", *,
#              kwarg1: int, kwarg2: str = "7i", kwarg3: str, **kwargss):
#     pass
# op = Operation(func).func(
#     [None], 5, MandatoryArgTypeContainer[int], "uuu",
#     MandatoryArgTypeContainer[str], 3, OptionalArgTypeContainer[str], 5,
#     kwarg1=9, kwarg3=PosArg(MandatoryArgTypeContainer[str], 3), kw100=90)
#
# actual_result = (([None], 5, 1, "uuu", "tt", 3, 4, "opt", 5),                  #args
#                  {"kwarg1": 9, "kwarg2": "7i", "kwarg3": "lll", "kw100": 90})  #kwargs






# print(getcallargs(func, [None], 5, "uuu", MandatoryArgTypeContainer[str], 3, kwarg1=9, kw100=90))
