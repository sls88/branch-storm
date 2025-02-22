from inspect import Parameter
from typing import Any, Dict, Optional, Tuple, Type, Union

from typeguard import check_type, TypeCheckError, CollectionCheckStrategy

from src.dataclasses import Param
from src.type_containers import SeqIdenticalTypesContainer, MandatoryArgTypeContainer, OptionalArgTypeContainer
from src.utils.formatters import LoggerBuilder

ArgTypeContainer = Type[Union[MandatoryArgTypeContainer, OptionalArgTypeContainer]]

log = LoggerBuilder().build()


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


class GetPosArg:
    def __init__(self, arg_type: ArgTypeContainer, number_position: int) -> None:
        self.arg_type = arg_type
        self.number_position = number_position

    def _validate(self) -> Optional[str]:
        if not is_it_arg_type(self.arg_type):
            return "Should be MandatoryArgTypeContainer or OptionalArgTypeContainer only"
        a_type = get_args_from_arg_type(self.arg_type)
        if a_type and len(a_type) > 1:
            return "Only one type possible to use for one keyword argument"
        elif a_type and is_it_seq_ident_types(a_type[0]):
            return "You cannot use a type container of the argument sequence for a single keyword argument"
        elif not isinstance(self.number_position, int):
            return "The number_position should be int type only"
        elif self.number_position < 1:
            return "The number_position should start from 1"


def get_args_kwargs(
        stack: str,
        params_wo_self: Dict[str, Parameter],
        args: Tuple,
        kwargs: Dict[str, Any],
        input_data: Tuple,
        hide_init_inf_from_logs: bool = False) -> Tuple[Tuple, Dict[str, Any], Optional[Tuple]]:
    params = enriched_params(params_wo_self, kwargs)
    kwargs_check(stack, params, kwargs)
    input_data, kwargs = replace_kwargs_to_data(input_data, kwargs)
    kw_params = add_kw_only_to_kwargs(params, kwargs)
    kw_params = get_exected_types_for_kwargs(kw_params)
    args_check(stack, params, args, kwargs)
    arg_params = get_args_params(params_wo_self, args, kwargs)
    check_type_containers(stack, arg_params)
    rem_data, arg_params = assign_values(input_data, arg_params)
    # print("arg_params", arg_params)
    check_mand_after_opt_at_container(stack, arg_params, kw_params)
    arg_params, kw_params = fill_default_values_or_raise_err(
        stack, arg_params, kw_params)
    # check_empty_params_if_type(arg_params, kw_params)
    arg_params, kw_params = check_types_and_get_values(
        stack, arg_params, kw_params)
    # check_empty_arg_values(arg_params)
    rem_data = None if not rem_data else rem_data
    args = get_args(arg_params)
    kwargs = get_kwargs(kw_params)

    LogMessageCreator.do_log_message(stack, args, kwargs, hide_init_inf_from_logs)
    return args, kwargs, rem_data


def check_type_containers(stack: str, arg_params: Dict[str, Param]) -> None:
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
            f"Stack: {stack}. Type containers of single positional arguments were\n"
            f"found that should receive more than one type.\n"
            f"Len: {len(incorrect_containers)}; Incorrect type containers: {incorrect_containers}.\n"
            f"You can use a Union type, but not a type tuple.\n"
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
            f"Stack: {stack}. Len {len(err_containers)}, Args map: {err_containers}\n"
            f"A container for the type of a mandatory argument cannot be passed after an optional one.\n"
            f"If the key is of type int this means the position number of\n"
            f"the positional arguments in *args tuple")


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
                param.kind == "VAR_POSITIONAL":
            continue
        elif (arg_empty_cond and param.type_container == 'optional' and
              param.def_val == Parameter.empty) or (
                arg_empty_cond and param.type_container == 'mandatory'):
            args_not_enough[name] = param.type_container
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
            f"Stack: {stack}. Len: {len(not_enough)}, Args map: {not_enough}.\n"
            f"For the listed arguments you expected to receive data of the\n "
            f"corresponding types, but apparently they were not enough for\n "
            f"the call/initialization.\n"
            f"Maybe type container does not match the expected argument.\n"
            f"If the argument is optional, then if it is not received,\n"
            f"the function must have a default value for it.\n"
            f"If the key is of type int this means the position number of\n"
            f"the positional arguments in *args tuple")

    return arg_params, kw_params


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


def is_it_arg_type(arg: Any) -> Optional[str]:
    if "__dict__" in dir(arg) and "__origin__" in arg.__dict__ and \
            arg.__dict__["__origin__"] is MandatoryArgTypeContainer:
        return "mandatory"
    elif "__dict__" in dir(arg) and "__origin__" in arg.__dict__ and \
            arg.__dict__["__origin__"] is OptionalArgTypeContainer:
        return "optional"
    class_name = None
    try:
        class_name = arg.__name__
    except AttributeError:
            pass
    if class_name == "MandatoryArgTypeContainer":
        return "mandatory"
    elif class_name == "OptionalArgTypeContainer":
        return "optional"


def is_it_seq_ident_types(arg: Any) -> bool:
    if "__dict__" in dir(arg) and "__origin__" in arg.__dict__ and arg.__dict__["__origin__"] is SeqIdenticalTypesContainer:
        return True
    return False


def get_args_from_arg_type(
        type_container: ArgTypeContainer) -> Optional[Tuple]:
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
            type_container = is_it_arg_type(arg)
            new_kwargs[name] = Param(
                elem, arg.arg_type, kind="KEYWORD_ONLY", type_container=type_container)
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


def get_new_arg_name(name: Any, count: Union[int, float]) -> Union[str, int, float]:
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


def get_type_container(param: Param) -> Tuple[Union[Parameter, str], Param]:
    type_container = is_it_arg_type(param.type)
    if not type_container:
        if is_it_seq_ident_types(param.type):
            param.type = MandatoryArgTypeContainer[param.type]
            type_container = "mandatory"
        else:
            type_container = Parameter.empty
    return type_container, param


def process_pos_arg(
        arg_name: Union[str, int, float],
        param: Param,
        input_data: Tuple,
        type_container: Union[Parameter, str]) -> Tuple[Dict, Param, Tuple]:
    new_param_map = {}
    if param.type != Parameter.empty:
        a_type = get_args_from_arg_type(param.type)
        elem, input_data = get_element(input_data, 1)
        if a_type:
            param.arg = elem
            param.type = a_type[0]
            param.type_container = type_container
        else:
            param.arg = Parameter.empty
            param.type = Parameter.empty
            param.value = elem
            param.type_container = type_container
    new_param_map[arg_name] = param

    return new_param_map, param, input_data


def process_many_types(
        types: Tuple,
        seq_num: Union[int, float],
        kind: str,
        input_data: Tuple,
        type_container: Union[Parameter, str]):
    new_param_map = {}
    for a_type in types:
        if not is_it_seq_ident_types(a_type):
            elem, input_data = get_element(input_data, 1)
            seq_num = int(seq_num) + 1
            new_param_map[seq_num] = Param(
                elem, a_type, kind=kind, type_container=type_container)
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
                    value=elem, kind=kind,
                    type_container=type_container)
            if not execution_flag:
                seq_num = int(seq_num) + 1
                new_param_map[seq_num] = Param(
                    arg=Parameter.empty, type=seq_type, kind=kind,
                    type_container=type_container)
            elem = () if elem == Parameter.empty else (elem,)
            input_data = (*elem, *input_data)

    return new_param_map, seq_num, input_data


def process_var_pos_arg(
        seq_num: Union[int, float],
        param: Param,
        input_data: Tuple,
        type_container: Union[Parameter, str]) -> Tuple[Dict, Param, Tuple, Union[int, float]]:
    new_param_map = {}
    kind = "VAR_POSITIONAL"
    if param.type == Parameter.empty and param.value == Parameter.empty:
        pass
    elif param.type == Parameter.empty and param.value != Parameter.empty:
        seq_num = int(seq_num) + 1
        new_param_map[int(seq_num)] = param
    elif is_it_arg_type(param.type) and not get_args_from_arg_type(param.type):
        while input_data:
            elem, input_data = get_element(input_data, 1)
            param = Param(value=elem, kind=kind,
                          type_container=type_container)
            seq_num = int(seq_num) + 1
            new_param_map[int(seq_num)] = param
    else:
        types = get_args_from_arg_type(param.type)
        if types:
            res, seq_num, input_data = process_many_types(
                types, seq_num, kind, input_data, type_container)
            new_param_map = {**new_param_map, **res}
        else:
            while input_data:
                elem, input_data = get_element(input_data, 1)
                param = Param(value=elem, kind=kind,
                              type_container=type_container)
                seq_num = int(seq_num) + 1
                new_param_map[int(seq_num)] = param

    return new_param_map, param, input_data, seq_num


def assign_values(
        input_data: Tuple,
        arg_params: Dict[str, Param]) -> Tuple[Tuple, Dict[str, Param]]:
    seq_num = 0
    new_param_map = {}
    for name, param in arg_params.items():
        name = get_new_arg_name(name, seq_num)
        type_container, param = get_type_container(param)
        if param.kind == "POSITIONAL_ONLY":
            res, param, input_data = process_pos_arg(name, param, input_data, type_container)
            new_param_map = {**new_param_map, **res}
        elif param.kind == "VAR_POSITIONAL":
            res, param, input_data, seq_num = process_var_pos_arg(
                seq_num, param, input_data, type_container)
            new_param_map = {**new_param_map, **res}

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


def kwargs_check(
        stack: str,
        params: Dict[str, Param],
        kwargs: Dict[str, Any]) -> None:
    var_keyword = any(map(lambda x: x.kind == "VAR_KEYWORD", params.values()))

    err_kwargs = {}
    for name, value in kwargs.items():
        if isinstance(value, GetPosArg):
            validation_res = value._validate()
            if validation_res:
                err_kwargs[name] = validation_res

    if err_kwargs:
        raise ValueError(f"Stack: {stack}. There was found incorrect kwargs. "
                         f"Len: {len(err_kwargs)}; kwargs: {err_kwargs}, where key = error message.")

    not_used_kwargs = {}
    for name, value in kwargs.items():
        if name not in params and not var_keyword:
            not_used_kwargs[name] = value
    if not_used_kwargs:
        hidden_kwargs = {name: type(val) for name, val in not_used_kwargs.items()}
        raise ValueError(f"Stack: {stack}. There was found kwargs not used in call/init. "
                         f"Len: {len(not_used_kwargs)}; kwargs: {hidden_kwargs}")

    mand_kwargs_not_enough = []
    for name, param in params.items():
        if param.kind == "KEYWORD_ONLY" and param.def_val == Parameter.empty and name not in kwargs:
            mand_kwargs_not_enough.append(name)

    if mand_kwargs_not_enough:
        raise ValueError(
            f"Stack: {stack}. Mandatory kwargs were found that were not passed to the call/init. "
            f"Len {len(mand_kwargs_not_enough)}; kwargs names: {mand_kwargs_not_enough}")


def args_check(
        stack: str,
        params: Dict[str, Param],
        args: Tuple,
        kwargs: Dict[str, Any]) -> None:
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
        raise ValueError(
            f"Stack: {stack}. There was found args not used in call/init. "
            f"Len: {len(args)}; args: {args_types}")

    if mand_args_not_enough:
        raise ValueError(
            f"Stack: {stack}. Mandatory args were found that were not passed to the call/init. "
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
        raise TypeError(f"Stack: {stack}. Argument mismatches with their types were found:\n"
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
