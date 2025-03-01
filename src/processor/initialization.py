import uuid
from dataclasses import dataclass
from inspect import Parameter
from typing import Any, Dict, Optional, Tuple, Type, Union

from typeguard import check_type, TypeCheckError, CollectionCheckStrategy

from src.launch_operations.errors import EmptyDataError
from src.type_containers import MandatoryArgTypeContainer, OptionalArgTypeContainer
from src.utils.formatters import LoggerBuilder

ArgTypeContainer = Union[Type[Union[
    MandatoryArgTypeContainer, OptionalArgTypeContainer]],
    MandatoryArgTypeContainer, OptionalArgTypeContainer]

log = LoggerBuilder().build()


@dataclass
class Param:
    arg: Any = Parameter.empty
    type: Any = Parameter.empty
    value: Any = Parameter.empty
    kind: str = Parameter.empty
    def_val: Any = Parameter.empty
    type_container: str = Parameter.empty


class LogMessageCreator:
    @staticmethod
    def do_log_message(
            stack: str,
            args: Tuple,
            kwargs: Dict[str, Any],
            hide_init_inf_from_logs: bool = False):
        args_for_log = tuple([type(arg) for arg in args])
        kw_for_log = LogMessageCreator._get_kwargs_for_log(kwargs)

        if not (args_for_log or kw_for_log):
            call_message = "\nThe call will be made without positional or keyword arguments."
        elif args_for_log and not kw_for_log:
            call_message = f"\nThe call will be made with positional arguments: " \
                           f"{args_for_log} and without keyword arguments."
        elif kw_for_log and not args_for_log:
            call_message = f"\nThe call will be made with keyword arguments: " \
                           f"{kw_for_log} and without positional arguments."
        else:
            call_message = f"\nThe call will be made with positional arguments: {args_for_log} " \
                           f"and keyword arguments: {kw_for_log}."

        if hide_init_inf_from_logs:
            call_message = ""

        log.info(f"Operation: {stack}{call_message}")

    @staticmethod
    def _get_kwargs_for_log(kwargs: Dict[str, Any]) -> Dict[str, Any]:
        kw_for_log = {}
        for key, value in kwargs.items():
            kw_for_log[key] = type(value)

        return kw_for_log


def get_args_kwargs(
        stack: str,
        params_wo_self: Dict[str, Parameter],
        args: Tuple,
        kwargs: Dict[str, Any],
        input_data: Tuple,
        hide_init_inf_from_logs: bool = False) -> Tuple[Tuple, Dict[str, Any], Optional[Tuple]]:
    validate_type_containers(stack, args, kwargs)
    input_data, args, kwargs = fill_type_containers_to_pos_data(
        stack, input_data, args, kwargs)
    params = enrich_params(params_wo_self)
    params = upd_params_by_keyword(params, kwargs)
    arg_params, kw_params = separate_params(params)
    check_len_args(stack, arg_params, args)
    check_len_kwargs(stack, kw_params, kwargs)
    arg_params = place_type_value_args(arg_params, args)
    kw_params = place_type_value_kw(kw_params, kwargs)
    check_sequence_in_args(stack, arg_params)
    check_mand_after_opt_at_container(stack, arg_params, kw_params)
    arg_params, kw_params = fill_params_in_init_type_containers(
        arg_params, kw_params)
    input_data, arg_params = assign_arg_values(input_data, arg_params)
    rem_data, kw_params = assign_kwarg_values(input_data, kw_params)
    arg_params, kw_params = fill_default_values_or_raise_err(
        stack, arg_params, kw_params)
    arg_params, kw_params = check_types_and_get_values(
        stack, arg_params, kw_params)
    rem_data = None if not rem_data else rem_data
    args = get_args(arg_params)
    kwargs = get_kwargs(kw_params)

    LogMessageCreator.do_log_message(stack, args, kwargs, hide_init_inf_from_logs)
    return args, kwargs, rem_data


def check_sequence_in_args(
        stack: str,
        arg_params: Dict[str, Param]) -> None:
    seq_for_args = []
    for name, param in arg_params.items():
        type_container = is_it_init_arg_type(param.type)
        if type_container:
            if param.type.is_it_seq_ident_types and param.kind != "VAR_POSITIONAL":
                seq_for_args.append(name)

    if seq_for_args:
        raise TypeError(
            f"Operation: {stack}\n"
            f"Positional arguments were found that attempted to assign a\n"
            f"sequence of identical types. Len: {len(seq_for_args)}\n"
            f"Arguments names: {seq_for_args}\n"
            f"Only var_positional arguments can consume sequences of input data.\n"
            f"Set seq=False (default)")


def assign_arg_values(
        input_data: Tuple,
        arg_params: Dict[str, Param]) -> Tuple[Tuple, Dict[str, Param]]:
    seq_num: Union[int, float] = 0
    new_param_map = {}
    kind = "POSITIONAL_ONLY"
    for name, param in arg_params.items():
        if param.kind == "VAR_POSITIONAL":
            seq_num = int(seq_num + 1)
            name = seq_num
        type_container = is_it_arg_type(param.type)
        if type_container:
            a_type = get_args_from_arg_type(param.type)
            if a_type and is_it_init_arg_type(param.type) and param.type.is_it_seq_ident_types:
                execution_flag = False
                while True:
                    elem, input_data = get_first_element(input_data)
                    strategy = CollectionCheckStrategy.ALL_ITEMS
                    try:
                        check_type(elem, a_type,
                                   collection_check_strategy=strategy)
                        execution_flag = True
                    except TypeCheckError:
                        break
                    seq_num = round(seq_num + 0.1, 3)
                    new_param_map[seq_num] = Param(
                        value=elem, kind=kind,
                        type_container=type_container)
                if not execution_flag:
                    seq_num = int(seq_num)
                    new_param_map[seq_num] = Param(
                        arg=Parameter.empty, type=a_type, kind=kind,
                        type_container=type_container)
                elem = () if elem == Parameter.empty else (elem,)
                input_data = (*elem, *input_data)
            elif not a_type and is_it_init_arg_type(param.type) and param.type.is_it_seq_ident_types:
                while input_data:
                    elem, input_data = get_first_element(input_data)
                    param = Param(value=elem, kind=kind,
                                  type_container=type_container)
                    seq_num = round(seq_num + 0.1, 3)
                    new_param_map[seq_num ] = param
            else:
                elem, input_data = get_first_element(input_data)
                if a_type:
                    param.arg = elem
                    param.type = a_type
                else:
                    param.arg = Parameter.empty
                    param.type = Parameter.empty
                    param.value = elem
                new_param_map[name] = param
        else:
            new_param_map[name] = param

    return input_data, new_param_map


def enrich_params(
        params: Dict[str, Union[Parameter, Param]]) -> Dict[str, Param]:
    for name, param in params.items():
        kind = param.kind.name
        def_val = param.default
        params[name] = Param(kind=kind, def_val=def_val)
    return params


def upd_params_by_keyword(
        params: Dict[str, Param],
        kwargs: Dict[str, Any]) -> Dict[str, Param]:
    kw_flag = False
    for name, param in params.items():
        if name in kwargs:
            kw_flag = True
        if kw_flag and not param.kind == "VAR_KEYWORD":
            param.kind = "KEYWORD_ONLY"
            params[name] = param
    return params


def place_type_value_kw(
        kw_params: Dict[str, Param],
        kwargs: Dict[str, Any]) -> Dict[str, Param]:
    kw_params = {name: arg for name, arg in kw_params.items()
                 if arg.kind != "VAR_KEYWORD"}
    for name, arg in kwargs.items():
        type_container = is_it_arg_type(arg)
        if not type_container:
            type_container = Parameter.empty
        el_type, value = get_type_value(arg)
        if name in kw_params:
            param = kw_params[name]
            param.type = el_type
            param.value = value
            param.type_container = type_container
        else:
            kw_params[name] = Param(
                type=el_type, value=value, kind="KEYWORD_ONLY", type_container=type_container)
    return kw_params


def place_type_value_args(
        arg_params: Dict[str, Param],
        args: Tuple) -> Dict[str, Param]:
    arg_params = {name: arg for name, arg in arg_params.items()
                 if arg.kind != "VAR_POSITIONAL"}
    for name, arg in arg_params.items():
        elem, args = get_first_element(args)
        type_container = is_it_arg_type(elem)
        if not type_container:
            type_container = Parameter.empty
        el_type, value = get_type_value(elem)
        arg.type = el_type
        arg.value = value
        arg.type_container = type_container
        arg_params[name] = arg

    if args:
        counter = 1
        while args:
            elem, args = get_first_element(args)
            type_container = is_it_arg_type(elem)
            if not type_container:
                type_container = Parameter.empty
            el_type, value = get_type_value(elem)
            name = f"{counter}_pos_arg"
            arg_params[name] = Param(
                type=el_type,
                value=value,
                kind="VAR_POSITIONAL",
                type_container=type_container)
            counter += 1

    return arg_params


def separate_params(
        params: Dict[str, Param]) -> Tuple[Dict[str, Param], Dict[str, Param]]:
    args_params = {}
    kw_params = {}
    for name, param in params.items():
        if param.kind in ["KEYWORD_ONLY", "VAR_KEYWORD"]:
            kw_params[name] = param
        elif param.kind == "VAR_POSITIONAL":
            args_params[name] = param
        else:
            param.kind = "POSITIONAL_ONLY"
            args_params[name] = param
    return args_params, kw_params


def fill_type_containers_to_pos_data(
        stack: str,
        input_data: Tuple,
        args: Tuple,
        kwargs: Dict[str, Any]) -> Tuple[Tuple, Tuple, Dict[str, Any]]:
    len_inp_data = len(input_data)
    unique_id = str(uuid.uuid4())

    args_not_enough = {}
    new_args = []
    for num, arg in enumerate(args, 1):
        type_container = is_it_init_arg_type(arg)
        if type_container:
            if arg.number_position:
                elem, input_data = replace_and_get_elem_by_pos(input_data, arg.number_position, unique_id)
                if elem == Parameter.empty and type_container == "mandatory":
                    args_not_enough[num] = arg.number_position
                arg.par_value = elem
        new_args.append(arg)
    args = tuple(new_args)

    if args_not_enough:
        raise EmptyDataError(
            f"Operation: {stack}\n"
            f"For mandatory positional arguments, "
            f"the position numbers of the input data were declared,\n"
            f"but there was not enough data. Len: {len(args_not_enough)},\n"
            f"Position arguments: {args_not_enough},\n"
            f"where key = ordinal number of argument, "
            f"value = declared position.\n"
            f"Total length of input tuple: {len_inp_data}.")

    kwargs_not_enough = {}
    seq_for_kwargs = []
    for name, arg in kwargs.items():
        type_container = is_it_init_arg_type(arg)
        if type_container:
            if arg.number_position:
                elem, input_data = replace_and_get_elem_by_pos(input_data, arg.number_position, unique_id)
                if elem == Parameter.empty and type_container == "mandatory":
                    kwargs_not_enough[name] = arg.number_position
                arg.par_value = elem
                kwargs[name] = arg
            elif arg.is_it_seq_ident_types:
                seq_for_kwargs.append(name)

    if kwargs_not_enough:
        raise EmptyDataError(
            f"Operation: {stack}\n"
            f"For mandatory keyword arguments, "
            f"the position numbers of the input data were declared,\n"
            f"but there was not enough data. Len: {len(kwargs_not_enough)},\n"
            f"Keyword arguments: {kwargs_not_enough},\n"
            f"where key = argument name, value = declared position.\n"
            f"Total length of input data tuple: {len_inp_data}.")

    if seq_for_kwargs:
        raise TypeError(
            f"Operation: {stack}\n"
            f"Keyword arguments were found that attempted to assign a\n"
            f"sequence of identical types. Len: {len(seq_for_kwargs)}\n"
            f"Arguments names: {seq_for_kwargs}\n"
            f"A keyword argument can only have one type. Sequences can\n"
            f"only be passed for positional arguments. Set seq=False (default)")

    input_data = tuple(filter(lambda x: x != unique_id, list(input_data)))
    return input_data, args, kwargs


def fill_params(
        params: Dict[str, Param]) -> Dict[str, Param]:
    for name, param in params.items():
        if is_it_init_arg_type(param.type):
            if param.type.par_value != Parameter.empty:
                if param.type.par_type == Parameter.empty:
                    param.value = param.type.par_value
                    param.type = Parameter.empty
                else:
                    param.arg = param.type.par_value
                    param.type = param.type.par_type
                params[name] = param

    return params


def fill_params_in_init_type_containers(
        arg_params: Dict[str, Param],
        kw_params: Dict[str, Param]) -> Tuple[Dict[str, Param], Dict[str, Param]]:
    arg_params = fill_params(arg_params)
    kw_params = fill_params(kw_params)

    return arg_params, kw_params


def check_mand_after_opt_at_container(
        stack: str,
        arg_params: Dict[str, Param],
        kw_params: Dict[str, Param]) -> None:
    err_containers = {}
    params = {**arg_params, **kw_params}
    opt_flag = False
    for name, param in params.items():
        if param.type_container == 'optional':
            opt_flag = True
        if param.type_container == 'mandatory' and opt_flag:
            err_containers[name] = param.type_container

    if err_containers:
        raise TypeError(
            f"Operation: {stack}. Len {len(err_containers)}, Args map: {err_containers}\n"
            f"A container for the type of a mandatory argument cannot be passed after an optional one.\n")


def fill_def_values(params: Dict[str, Param]) -> Tuple[Dict[str, str], Dict[str, Param]]:
    args_not_enough = {}
    new_params = {}
    for name, param in params.items():
        arg_empty_cond = param.type != Parameter.empty and param.arg == Parameter.empty
        if arg_empty_cond and param.type_container == 'optional' and \
                param.def_val != Parameter.empty:
            param.value = param.def_val
            param.type = Parameter.empty
            new_params[name] = param
        elif arg_empty_cond and param.type_container == 'optional' and \
                param.kind == "KEYWORD_ONLY":
            continue
        elif arg_empty_cond and param.type_container == 'optional' and \
                param.kind == "VAR_POSITIONAL":
            continue
        elif (arg_empty_cond and param.type_container == 'optional' and
              param.def_val == Parameter.empty) or (
                arg_empty_cond and param.type_container == 'mandatory'):
            args_not_enough[name] = param.type_container
        elif param.value == Parameter.empty and param.def_val != Parameter.empty:
            param.value = param.def_val
        new_params[name] = param

    return args_not_enough, new_params


def fill_default_values_or_raise_err(
        stack: str,
        arg_params: Dict[str, Param],
        kw_params: Dict[str, Param]) -> Tuple[Dict[str, Param], Dict[str, Param]]:
    not_enough_args, arg_params = fill_def_values(arg_params)
    not_enough_kw, kw_params = fill_def_values(kw_params)
    not_enough = {**not_enough_args, **not_enough_kw}

    if not_enough:
        raise TypeError(
            f"Operation: {stack}. Len: {len(not_enough)}, Args map: {not_enough}.\n"
            f"For the listed arguments you expected to receive data of the\n "
            f"corresponding types, but apparently they were not enough for\n "
            f"the call/initialization.\n"
            f"Maybe type container does not match the expected argument.\n"
            f"If the argument is optional, then if it is not received,\n"
            f"the function must have a default value for it.\n"
            f"If the key is of type int this means the position number of\n"
            f"the positional arguments in *args tuple")

    return arg_params, kw_params


def is_it_arg_type(arg: Any) -> Optional[str]:
    if "__dict__" in dir(arg) and "__origin__" in arg.__dict__ and \
            arg.__dict__["__origin__"] is OptionalArgTypeContainer or \
            isinstance(arg, OptionalArgTypeContainer):
        return "optional"
    elif "__dict__" in dir(arg) and "__origin__" in arg.__dict__ and \
            arg.__dict__["__origin__"] is MandatoryArgTypeContainer or \
            isinstance(arg, MandatoryArgTypeContainer):
        return "mandatory"
    class_name = None
    try:
        class_name = arg.__name__
    except AttributeError:
            pass
    if class_name == "MandatoryArgTypeContainer":
        return "mandatory"
    elif class_name == "OptionalArgTypeContainer":
        return "optional"


def is_it_init_arg_type(arg: Any) -> Optional[str]:
    if isinstance(arg, OptionalArgTypeContainer):
        return "optional"
    elif isinstance(arg, MandatoryArgTypeContainer):
        return "mandatory"


def get_args_from_arg_type(
        type_container: ArgTypeContainer) -> Optional[Tuple]:
    if isinstance(type_container, MandatoryArgTypeContainer):
        if type_container.par_type == Parameter.empty:
            return None
        return type_container.par_type
    if '__args__' in type_container.__dict__:
        return type_container.__dict__['__args__'][0]
    return None


def get_first_element(input_data: Tuple) -> Tuple[Any, Tuple]:
    if not len(input_data):
        return Parameter.empty, input_data
    return input_data[0], input_data[1:]


def replace_and_get_elem_by_pos(input_data: Tuple, elem_pos: int, replacement: Any) -> Tuple[Any, Tuple]:
    if elem_pos <= 0 or elem_pos > len(input_data):
        return Parameter.empty, input_data

    elem = input_data[elem_pos - 1]
    input_data = list(input_data)
    input_data[elem_pos - 1] = replacement
    return elem, tuple(input_data)


def assign_kwarg_values(
        input_data: Tuple,
        kw_params: Dict[str, Param]) -> Tuple[Tuple, Dict[str, Param]]:
    for name, param in kw_params.items():
        type_container = is_it_arg_type(param.type)
        if type_container:
            a_type = get_args_from_arg_type(param.type)
            elem, input_data = get_first_element(input_data)
            if a_type:
                param.arg = elem
                param.type = a_type
            else:
                param.arg = Parameter.empty
                param.type = Parameter.empty
                param.value = elem
            kw_params[name] = param

    return input_data, kw_params


def get_type_value(elem: Any) -> Tuple[Type, Any]:
    if is_it_arg_type(elem):
        return elem, Parameter.empty
    return Parameter.empty, elem


def validate_type_containers(
        stack: str,
        args: Tuple,
        kwargs: Dict[str, Any]) -> None:
    err_args = {}
    for num, arg in enumerate(args, 1):
        if is_it_init_arg_type(arg):
            validation_res = arg._validate()
            if validation_res:
                err_args[num] = validation_res

    err_kwargs = {}
    for name, value in kwargs.items():
        if is_it_init_arg_type(value):
            validation_res = value._validate()
            if validation_res:
                err_kwargs[name] = validation_res

    if err_args or err_kwargs:
        raise ValueError(
            f"Operation: {stack}. There was found incorrect type_containers.\n"
            f"Len: {len({**err_args, **err_kwargs})}; kwargs: {err_kwargs},\n"
            f"where key = argument name, value = error message;\n"
            f"args: {err_args}, where key = argument number position,\n"
            f"value = error message.")


def check_len_kwargs(
        stack: str,
        kw_params: Dict[str, Param],
        kwargs: Dict[str, Any]) -> None:
    var_keyword = any(map(lambda x: x.kind == "VAR_KEYWORD", kw_params.values()))

    not_used_kwargs = {}
    for name, value in kwargs.items():
        if name not in kw_params and not var_keyword:
            not_used_kwargs[name] = value
    if not_used_kwargs:
        hidden_kwargs = {name: type(val) for name, val in not_used_kwargs.items()}
        raise ValueError(f"Operation: {stack}. There was found kwargs not used in call/init. "
                         f"Len: {len(not_used_kwargs)}; kwargs: {hidden_kwargs}")

    mand_kwargs_not_enough = []
    for name, param in kw_params.items():
        if param.kind == "KEYWORD_ONLY" and param.def_val == Parameter.empty and name not in kwargs:
            mand_kwargs_not_enough.append(name)

    if mand_kwargs_not_enough:
        raise ValueError(
            f"Operation: {stack}. Mandatory kwargs were found that were not passed to the call/init. "
            f"Len {len(mand_kwargs_not_enough)}; kwargs names: {mand_kwargs_not_enough}")


def check_len_args(
        stack: str,
        arg_params: Dict[str, Param],
        args: Tuple) -> None:
    var_positional = any(map(lambda x: x.kind == "VAR_POSITIONAL", arg_params.values()))

    mand_args_not_enough = []
    for name, param in arg_params.items():
        if param.kind == "POSITIONAL_ONLY":
            if param.def_val == Parameter.empty and not args:
                mand_args_not_enough.append(name)
            _, args = get_first_element(args)
    args_types = tuple([type(arg) for arg in args])

    if not var_positional and args:
        raise ValueError(
            f"Operation: {stack}. There was found args not used in call/init. "
            f"Len: {len(args)}; args: {args_types}")

    if mand_args_not_enough:
        raise ValueError(
            f"Operation: {stack}. Mandatory args were found that were not passed to the call/init. "
            f"Len {len(mand_args_not_enough)}; arg names: {mand_args_not_enough}")


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


def check_types_and_get_values(
        stack: str,
        arg_params: Dict[str, Param],
        kw_params: Dict[str, Param]) -> Tuple[Dict[str, Param], Dict[str, Param]]:
    for name, param in arg_params.items():
        if param.value == Parameter.empty and param.def_val != Parameter.empty:
            param.value = param.def_val

    kw_type_err = check_arg_type(kw_params)
    args_type_err = check_arg_type(arg_params)
    if kw_type_err or args_type_err:
        common_map = {**args_type_err, **kw_type_err}
        raise TypeError(f"Operation: {stack}. Argument mismatches with their types were found:\n"
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
