import uuid
from inspect import Parameter
from typing import Any, Dict, Optional, Tuple, Type, Union

from typeguard import check_type, TypeCheckError, CollectionCheckStrategy

from src.dataclasses import Param
from src.type_containers import ArgTypeContainer, SeqIdenticalTypesContainer


class GetPosArg:
    def __init__(self, arg_type: Type[ArgTypeContainer], number_position: int) -> None:
        self.arg_type = arg_type
        self.number_position = number_position
        if not is_it_arg_type(arg_type):
            raise TypeError("Should be ArgTypeContainer only")
        a_type = get_args_from_arg_type(arg_type)
        if a_type and len(a_type) > 1:
            raise TypeError("Only one type possible to use for one keyword argument")
        elif a_type and is_it_seq_ident_types(a_type[0]):
            raise TypeError("You cannot use a type container of "
                            "the argument sequence for a single keyword argument")
        elif not isinstance(self.number_position, int):
            raise TypeError("The number_position should be int type only")
        elif self.number_position < 1:
            raise ValueError("The number_position should start from 1")


def get_args_kwargs(params_wo_self: Dict[str, Parameter],
                    args: Tuple, kwargs: Dict[str, Any],
                    input_data: Tuple) -> Tuple[Tuple, Dict[str, Any], Optional[Tuple]]:
    params = enriched_params(params_wo_self, kwargs)
    # params_kind = {name: par.kind for name, par in params.items()}
    # print("params_kind: ", params_kind)
    kwargs_check(params, kwargs)
    input_data, kwargs = replace_kwargs_to_data(input_data, kwargs)
    kw_params = add_kw_only_to_kwargs(params, kwargs)
    kw_params = get_exected_types_for_kwargs(kw_params)
    args_check(params, args, kwargs)
    arg_params = get_args_params(params_wo_self, args, kwargs)
    print("arg_params", arg_params)
    check_type_containers(arg_params)
    rem_data, arg_params = assign_values(input_data, arg_params)
    check_empty_params_if_type(arg_params, kw_params)
    arg_params, kw_params = check_types_and_get_values(arg_params, kw_params)
    check_empty_arg_values(arg_params)
    rem_data = None if not rem_data else rem_data
    return get_args(arg_params), get_kwargs(kw_params), rem_data


def check_type_containers(arg_params: Dict[str, Param]) -> None:
    incorrect_containers = {}
    for name, param in arg_params.items():
        if param.kind == "VAR_POSITIONAL":
            continue
        elif is_it_seq_ident_types(param.type):
            incorrect_containers[name] = "sequence_for_one_arg"
        elif is_it_arg_type(param.type):
            a_type = get_args_from_arg_type(param.type)
            if a_type and len(a_type) > 1:
                incorrect_containers[name] = a_type

    if incorrect_containers:
        raise TypeError(
            f"Type containers of single positional arguments were "
            f"found that should receive more than one type. \n"
            f"Len: {len(incorrect_containers)}; Incorrect_containers: {incorrect_containers}.\n"
            f"You can use a Union type, but not a type tuple. \n"
            f"Also, you cannot use a type container of a sequence of types for a single argument.")



def get_exected_types_for_kwargs(kw_params: Dict[str, Param]) -> Dict[str, Param]:
    for name, param in kw_params.items():
        if is_it_arg_type(param.type):
            a_type = get_args_from_arg_type(param.type)
            if a_type:
                param.type = a_type[0]
                kw_params[name] = param
            else:
                param.value = param.arg
                param.arg = Parameter.empty
                param.type = Parameter.empty
    return kw_params


def check_empty_params_if_type(
        arg_params: Dict[str, Param],
        kw_params: Dict[str, Param]) -> None:
    empty_params = {}
    for name, param in arg_params.items():
        if param.type != Parameter.empty and param.arg == Parameter.empty:
            empty_params[name] = param.type
    for name, param in kw_params.items():
        if param.type != Parameter.empty and param.arg == Parameter.empty:
            empty_params[name] = param.type

    if empty_params:
        raise TypeError(
            f"For the listed arguments you expected to receive data of the "
            f"corresponding types, but apparently they were not enough for "
            f"the call/initialization. Type map: {empty_params}. "
            f"If the key is of type int this means the position number of "
            f"the positional arguments in *args tuple")


def check_empty_arg_values(
        arg_params: Dict[str, Param]) -> None:
    empty_values = []
    for name, param in arg_params.items():
        if param.value == Parameter.empty:
            empty_values.append(name)

    if empty_values:
        raise TypeError(
            f"Arguments with empty values were found. "
            f"Len: {len(empty_values)}; Argument names: {empty_values}. \n"
            f"Perhaps you passed the type container incorrectly. "
            f"An empty type container for *args absorbs the entire "
            f"sequence of incoming data, after it you cannot accept "
            f"other type containers for positional arguments. \n"
            f"If the argument name is of type int this means the "
            f"position number of positional arguments in *args tuple")


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


def is_it_arg_type(arg: Any) -> bool:
    if "__dict__" in dir(arg) and "__origin__" in arg.__dict__ and arg.__dict__["__origin__"] is ArgTypeContainer:
        return True
    class_name = None
    try:
        class_name = arg.__name__
    except AttributeError:
            pass
    if class_name == "ArgTypeContainer":
        return True
    return False


def is_it_seq_ident_types(arg: Any) -> bool:
    if "__dict__" in dir(arg) and "__origin__" in arg.__dict__ and arg.__dict__["__origin__"] is SeqIdenticalTypesContainer:
        return True
    return False


def get_args_from_arg_type(
        type_container: Type[ArgTypeContainer]) -> Optional[Tuple]:
    if '__args__' in type_container.__dict__:
        return type_container.__dict__['__args__']
    return None


def replace_kwargs_to_data(
        input_data: Tuple,
        kwargs: Dict[str, Any]) -> Tuple[Tuple, Dict[str, Any]]:
    new_kwargs = {}
    drop_counter = 0
    for name, arg in kwargs.items():
        if not isinstance(arg, GetPosArg):
            new_kwargs[name] = Param(
                value=arg, kind="KEYWORD_ONLY")
        else:
            elem, input_data = get_element(
                input_data, arg.number_position - drop_counter)
            new_kwargs[name] = Param(
                elem, arg.arg_type, kind="KEYWORD_ONLY")
            drop_counter += 1
    return input_data, new_kwargs



def get_element(input_data: Tuple, elem_num: int):
    try:
        elem = input_data[elem_num - 1]
        input_data = input_data[:elem_num - 1] + input_data[elem_num:]
    except IndexError:
        return Parameter.empty, input_data
    return elem, input_data


def get_arg_silently(args: Tuple):
    return (Parameter.empty, ()) if not len(args) else (args[0], args[1:])


def get_new_arg_name(name: Any, count: Union[int, float]) -> Union[int, float]:
    if isinstance(name, int) or isinstance(name, float):
        return name
    splited_name = name.split("_")
    if len(splited_name) == 3:
        try:
            int(splited_name[0])
            return count
        except ValueError:
            pass
    return name


def assign_values(
        input_data: Tuple,
        arg_params: Dict[str, Param]) -> Tuple[Tuple, Dict[str, Param]]:
    seq_num = 0
    new_param_map = {}
    for name, param in arg_params.items():
        name = get_new_arg_name(name, seq_num)
        if param.kind == "POSITIONAL_ONLY":
            if param.type != Parameter.empty:
                a_type = get_args_from_arg_type(param.type)
                elem, input_data = get_element(input_data, 1)
                if a_type:
                    param.arg = elem
                    param.type = a_type[0]
                else:
                    param.arg = Parameter.empty
                    param.type = Parameter.empty
                    param.value = elem
            new_param_map[name] = param
        elif param.kind == "VAR_POSITIONAL":
            kind = "VAR_POSITIONAL"
            if is_it_seq_ident_types(param.type):
                param.type = ArgTypeContainer[param.type]
            if param.type == Parameter.empty and param.value == Parameter.empty:
                pass
            elif param.type == Parameter.empty and param.value != Parameter.empty:
                seq_num = int(seq_num) + 1
                new_param_map[int(seq_num)] = param
            elif is_it_arg_type(param.type) and not get_args_from_arg_type(param.type):
                while input_data:
                    elem, input_data = get_element(input_data, 1)
                    param = Param(value=elem, kind=kind)
                    seq_num = int(seq_num) + 1
                    new_param_map[int(seq_num)] = param
            else:
                types = get_args_from_arg_type(param.type)
                if types:
                    for a_type in types:
                        if not is_it_seq_ident_types(a_type):
                            elem, input_data = get_element(input_data, 1)
                            seq_num = int(seq_num) + 1
                            new_param_map[seq_num] = Param(elem, a_type, kind=kind)
                        elif is_it_seq_ident_types(a_type):
                            seq_type = get_args_from_arg_type(a_type)[0]
                            execution_flag = False
                            while True:
                                elem, input_data = get_element(input_data, 1)
                                strategy = CollectionCheckStrategy.ALL_ITEMS
                                try:
                                    check_type(elem, seq_type,
                                               collection_check_strategy=strategy)
                                    execution_flag = True
                                except TypeCheckError:
                                    break
                                seq_num = round(seq_num + 0.1, 3)
                                new_param_map[seq_num] = Param(
                                    value=elem, kind=kind)
                            if not execution_flag:
                                seq_num = int(seq_num) + 1
                                new_param_map[seq_num] = Param(
                                    arg=Parameter.empty, type=seq_type, kind=kind)
                            elem = () if elem == Parameter.empty else (elem,)
                            input_data = (*elem, *input_data)
                else:
                    while input_data:
                        elem, input_data = get_element(input_data, 1)
                        param = Param(value=elem, kind=kind)
                        seq_num = int(seq_num) + 1
                        new_param_map[int(seq_num)] = param

    return input_data, new_param_map


def get_type_value(elem: Any) -> Tuple[Type, Any]:
    if is_it_arg_type(elem) or is_it_seq_ident_types(elem):
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
            new_params[name] = Param(type=el_type, value=value, kind="VAR_POSITIONAL")
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
            strategy = CollectionCheckStrategy.ALL_ITEMS
            try:
                check_type(param.arg, param.type,
                           collection_check_strategy=strategy)
            except TypeCheckError:
                type_err[name] = (type(param.arg), param.type)

    return type_err


def fill_values(params: Dict[str, Param]) -> Dict[str, Param]:
    for name, param in params.items():
        if param.arg != Parameter.empty:
            param.value = param.arg
            params[name] = param
    return params


def check_types_and_get_values(arg_params: Dict[str, Param],
                               kw_params: Dict[str, Param]) -> Tuple[Dict[str, Param], Dict[str, Param]]:
    for name, param in arg_params.items():
        if param.value == Parameter.empty and param.def_val != Parameter.empty:
            param.value = param.def_val

    kw_type_err = check_arg_type(kw_params)
    args_type_err = check_arg_type(arg_params)
    if kw_type_err or args_type_err:
        common_map = {**args_type_err, **kw_type_err}
        raise TypeError(f"Argument mismatches with their types were found: \n"
                        f"Len: {len(common_map)}; Arg type map: {common_map}\n"
                        f"where dict(argument_name: tuple(actual_arg_type, expected_arg_type))\n"
                        f"If the argument name is of type int this means the "
                        f"positional arguments position number in *args tuple")

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
