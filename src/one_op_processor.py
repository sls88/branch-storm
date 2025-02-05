import uuid
from dataclasses import dataclass
from inspect import signature, Parameter
from typing import Any, Dict, List, Optional, Tuple, Callable, Type, OrderedDict, \
    TypeVar

from typeguard import TypeCheckError, check_type
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


TT = TypeVarTuple('TT')
T = TypeVar('T')

class ArgTypeContainer(Generic[Unpack[TT]]):
    pass


class SeqIdenticalTypesContainer(Generic[T]):
    pass


def is_it_arg_type(arg: Any) -> bool:
    if "__dict__" in dir(arg) and "__origin__" in arg.__dict__ and arg.__dict__["__origin__"] is ArgTypeContainer:
        return True
    return False


def is_it_seq_ident_types(arg: Any) -> bool:
    if "__dict__" in dir(arg) and "__origin__" in arg.__dict__ and arg.__dict__["__origin__"] is SeqIdenticalTypesContainer:
        return True
    return False


def get_args_from_arg_type(
        type_container: Type[ArgTypeContainer]) -> Tuple:
    return type_container.__dict__['__args__']


class PosArg:
    def __init__(self, arg_type: Type[ArgTypeContainer], number_position: int) -> None:
        self.arg_type = arg_type
        self.number_position = number_position
        if not is_it_arg_type(arg_type):
            raise TypeError
        elif len(get_args_from_arg_type(arg_type)) > 1:
            raise TypeError
        elif not isinstance(self.number_position, int):
            raise TypeError("The number_position should be int type only")
        elif self.number_position < 1:
            raise ValueError("The number_position should start from 1")


def replace_kwargs_to_data(
        input_data: Tuple,
        kwargs: Dict[str, Any]) -> Tuple[Tuple, Dict[str, Any]]:
    input_data, new_kwargs = list(input_data), {}
    uniq_uuid = str(uuid.uuid4())
    for name, arg in kwargs.items():
        if not isinstance(arg, PosArg):
            new_kwargs[name] = Param(
                value=arg, kind="KEYWORD_ONLY")
        else:
            new_kwargs[name] = Param(
                arg=input_data[arg.number_position - 1],
                type=arg.arg_type, kind="KEYWORD_ONLY")
            input_data[arg.number_position - 1] = uniq_uuid
    return tuple(list(filter(lambda x: x != uniq_uuid, input_data))), new_kwargs



def get_element(input_data: Tuple, elem_num: int):
    try:
        elem = input_data[elem_num - 1]
        input_data = input_data[:elem_num - 1] + input_data[elem_num:]
    except IndexError:
        return Parameter.empty, input_data
    return elem, input_data


@dataclass
class Param:
    arg: Any = Parameter.empty
    type: Any = Parameter.empty
    value: Any = Parameter.empty
    kind: Optional[str] = Parameter.empty
    def_val: Optional[Any] = Parameter.empty


def get_arg_silently(args: Tuple):
    return (Parameter.empty, ()) if not len(args) else (args[0], args[1:])


def assign_values(
        input_data: Tuple,
        arg_params: Dict[str, Param]) -> Tuple[Tuple, Dict[str, Param]]:
    seq_num = 1
    new_param_map = {}
    for name, param in arg_params.items():
        if param.kind == "POSITIONAL_ONLY":
            if param.type != Parameter.empty:
                a_type = get_args_from_arg_type(param.type)[0]
                elem, input_data = get_element(input_data, 1)
                param.arg = elem
                param.type = a_type
            new_param_map[name] = param
        elif param.kind == "VAR_POSITIONAL":
            if param.type == Parameter.empty:
                while input_data:
                    elem, input_data = get_element(input_data, 1)
                    param = Param(param.arg, param.type, elem,
                                   "POSITIONAL_ONLY")
                    new_param_map[int(seq_num)] = param
            else:
                types = get_args_from_arg_type(param.type)
                for a_type in types:
                    if not is_it_seq_ident_types(a_type):
                        elem, input_data = get_element(input_data, 1)
                        seq_num = int(seq_num) + 1
                        new_param_map[seq_num] = Param(elem, a_type, kind="POSITIONAL_ONLY")
                    elif is_it_seq_ident_types(a_type):
                        seq_type = get_args_from_arg_type(a_type)[0]
                        while True:
                            elem, input_data = get_element(input_data, 1)
                            try:
                                check_type(elem, seq_type)
                            except TypeCheckError:
                                break
                            seq_num = round(seq_num + 0.1, 3)
                            new_param_map[seq_num] = Param(
                                value=elem, kind="POSITIONAL_ONLY")

                        elem = () if elem == Parameter.empty else (elem,)
                        input_data = (*elem, *input_data)

    return input_data, new_param_map


def get_type_value(elem: Any) -> Tuple[Type, Any]:
    if is_it_arg_type(elem):
        return elem, Parameter.empty
    return Parameter.empty, elem


def get_args_params(params: Dict[str, Parameter], args: Tuple, kwargs: Dict[str, Param]) -> Dict[str, Param]:
    new_params = {}
    params_wo_kwargs = {name: val for name, val in params.items()
                        if name not in kwargs and not val.kind.name == "VAR_KEYWORD"}
    var_positional = False
    for name, param in params_wo_kwargs.items():
        elem, args = get_element(args, 1)
        kind = "POSITIONAL_ONLY"
        if param.kind.name == "VAR_POSITIONAL":
            kind = "VAR_POSITIONAL"
            var_positional = True
        el_type, value = get_type_value(elem)
        new_params[name] = Param(type=el_type, value=value, kind=kind, def_val=param.default)

    if args and var_positional:
        counter = 1
        while args:
            elem, args = get_element(args, 1)
            el_type, value = get_type_value(elem)
            name = f"{counter}_pos_arg"
            new_params[name] = Param(type=el_type, value=value, kind="POSITIONAL_ONLY")
            counter += 1

    return new_params



def kwargs_check(params: Dict[str, Param],
                 kwargs: Dict[str, Any]) -> None:
    var_keyword = any(map(lambda x: x.kind == "VAR_KEYWORD", params.values()))

    not_used_kwargs = {}
    for name, value in kwargs.items():
        if name not in params and not var_keyword:
            not_used_kwargs[name] = value
    if not_used_kwargs:
        hidden_kwargs = {name: type(val) for name, val in not_used_kwargs.items()}
        raise ValueError(f"There was found kwargs not used in call/init. "
                         f"Len: {len(not_used_kwargs)}; kwargs: {hidden_kwargs}")

    mand_kwargs_not_enough = []
    for name, param in params.items():
        if param.kind == "KEYWORD_ONLY" and param.def_val == Parameter.empty and name not in kwargs:
            mand_kwargs_not_enough.append(name)

    if mand_kwargs_not_enough:
        raise ValueError(f"Mandatory kwargs were found that were not passed to the call/init. "
                         f"Len {len(mand_kwargs_not_enough)}; kwargs names: {mand_kwargs_not_enough}")


def args_check(params: Dict[str, Param],
               args: Tuple, kwargs: Dict[str, Any]) -> None:
    params_wo_kwargs = {name: val for name, val in params.items() if name not in kwargs}
    var_positional = any(map(lambda x: x.kind == "VAR_POSITIONAL", params_wo_kwargs.values()))

    mand_args_not_enough = []
    for name, param in params_wo_kwargs.items():
        if param.kind in ["POSITIONAL_ONLY", "POSITIONAL_OR_KEYWORD"]:
            if param.def_val == Parameter.empty and not args:
                mand_args_not_enough.append(name)
            _, args = get_element(args, 1)
    args_types = tuple([type(arg) for arg in args])

    if not var_positional and args:
        raise ValueError(f"There was found args not used in call/init. "
                         f"Len: {len(args)}; args: {args_types}")

    if mand_args_not_enough:
        raise ValueError(f"Mandatory args were found that were not passed to the call/init. "
                         f"Len {len(mand_args_not_enough)}; arg names: {mand_args_not_enough}")


def add_kw_only_to_kwargs(params: Dict[str, Param], kwargs: Dict[str, Param]) -> Dict[str, Param]:
    for name, param in params.items():
        if param.kind == "KEYWORD_ONLY" and name not in kwargs:
            kwargs[name] = Param(value=param.def_val, kind="KEYWORD_ONLY", def_val=param.def_val)
    return kwargs


def change_kwargs_kind(params: Dict[str, Parameter], kwargs: Dict[str, Any]) -> Dict[str, Parameter]:
    kw_flag = False
    for name, param in params.items():
        if name in kwargs:
            kw_flag = True
        if kw_flag:
            param.kind.name = "KEYWORD_ONLY"
            params[name] = param
    return params


def check_arg_type(params: Dict[str, Param]) -> Dict[str, Tuple]:
    type_err = {}
    for name, param in params.items():
        if param.arg != Parameter.empty:
            try:
                check_type(param.arg, param.type)
            except TypeCheckError:
                type_err[name] = (param.arg, param.type)

    return type_err


def fill_values(params: Dict[str, Param]) -> Dict[str, Param]:
    for name, param in params.items():
        if param.arg != Parameter.empty:
            param.value = param.arg
            params[name] = param
    return params


def check_types_and_get_values(arg_params: Dict[str, Param],
                               kw_params: Dict[str, Param]) -> Tuple[Dict[str, Param], Dict[str, Param]]:
    for name, param in kw_params.items():
        if is_it_arg_type(param.type):
            param.type = get_args_from_arg_type(param.type)[0]
            kw_params[name] = param
    kw_type_err = check_arg_type(kw_params)
    args_type_err = check_arg_type(arg_params)
    if kw_type_err or args_type_err:
        raise TypeError

    return fill_values(arg_params), fill_values(kw_params)


def get_kwargs(kw_params: Dict[str, Param]) -> Dict[str, Any]:
    kwargs = {}
    for name, param in kw_params.items():
        kwargs[name] = param.value
    return kwargs


def get_args(arg_params: Dict[str, Param]) -> Tuple:
    args = []
    for name, param in arg_params.items():
        args.append(param.value)
    return tuple(args)


def enriched_params(params: Dict[str, Parameter], kwargs: Dict[str, Any]) -> Dict[str, Param]:
    kw_flag = False
    new_params = {}
    for name, param in params.items():
        if name in kwargs:
            kw_flag = True
        kind = param.kind.name
        if kw_flag and not param.kind.name == "VAR_KEYWORD":
            kind = "KEYWORD_ONLY"
        new_params[name] = Param(kind=kind, def_val=param.default)
    return new_params

class Operation:
    def __init__(self, class_or_func, instance: Optional[Any] = None, method: Optional[str] = None):
        self.class_or_func = class_or_func
        self.instance = instance
        self.method = method

    def func(self, *args, **kwargs):
        input_data = (1, "tt", "lll", "pppp", 4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F", 13)
        params_wo_self = {name: par for name, par in get_params_wo_self(self.class_or_func, False).items()}
        params = enriched_params(params_wo_self, kwargs)
        # params_kind = {name: par.kind for name, par in params.items()}
        # print("params_kind: ", params_kind)
        kwargs_check(params, kwargs)
        input_data, kwargs = replace_kwargs_to_data(input_data, kwargs)
        kw_params = add_kw_only_to_kwargs(params, kwargs)
        args_check(params, args, kwargs)
        arg_params = get_args_params(params_wo_self, args, kwargs)
        input_data, arg_params = assign_values(input_data, arg_params)
        arg_params, kw_params = check_types_and_get_values(arg_params, kw_params)
        kwargs = get_kwargs(kw_params)
        args = get_args(arg_params)
        print("rest_input_data: ", input_data)
        print()
        print("ARGS: ", args)
        print("KWARGS: ", kwargs)
        return args, kwargs

        # return self

input_data = (1, "tt", "lll", "pppp", 4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F", 13)

def func(arg1: List[None], arg2, arg3: int, arg4: str, arg5: str, arg6: int, *argss,
         kwarg1: int, kwarg2: str = "7i", kwarg3: str, **kwargss):
    print("SUCCESS")
    print(arg1, arg2, arg3, arg4, arg5, arg6, argss, kwarg1, kwarg2, kwarg3, kwargss)

op = Operation(func)
args, kwargs = op.func(
    [None], 5, ArgTypeContainer[int], "uuu", ArgTypeContainer[str], 3,
    ArgTypeContainer[float, SeqIdenticalTypesContainer[bool], str, str,
    SeqIdenticalTypesContainer[int]], "x5", ArgTypeContainer[str],
    kwarg1=9, kwarg3=PosArg(ArgTypeContainer[str], 3), kw100=90,
    kw200=PosArg(ArgTypeContainer[str], 4))

func(*args, **kwargs)


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


def abc(a, b, c=3):
    pass

# input_data = (1, "tt", "lll", "pppp", 4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F")
# print(getcallargs(func,[None], 5, 1, "uuu", "tt", 3, 4.0,
#                   True, True, True, "str1", "str2", 1, 2, 3, 4, "x5",
#                   kwarg1=9, kwarg3="lll", kw100=90, kw200="pppp"))
# args = ([None], 5, 1, "uuu", "tt", 3, 4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "x5")
# kwargs = {"kwarg1": 9, "kwarg3": "lll", "kw100": 90, "kw200": "pppp"}

# print(getcallargs(func, [None], 5, "uuu", MandatoryArgTypeContainer[str], 3, kwarg1=9, kw100=90))

# def func2(a, b, c = 3, *args):
#     print(a, b, c, args)
#     pass
# print(getcallargs(func2, 1, 2))
# print(func2(1, 2))

# def f1(a, b, *c):
#     pass
#
# op = Operation(f1).func(a=1, b=2, c={1: 1})
# f1(*(),**{'a': Param2(value=1, kind='KEYWORD_ONLY'), 'b': Param2(value=2, kind='KEYWORD_ONLY'), 'c': Param2(value=3, kind='KEYWORD_ONLY', def_val=3)})