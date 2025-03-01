from inspect import Parameter, signature
from typing import Any, Dict, Tuple, Callable, Type

from src.constants import PARAMETER_WAS_NOT_EXPANDED
from src.processor.initialization import is_it_init_arg_type
from src.utils.common import find_rw_inst
from src.utils.formatters import error_formatter, LoggerBuilder


log = LoggerBuilder().build()


def get_params_wo_self(func: Callable, remove_first: bool = True) -> Dict[str, Parameter]:
    """Parse function or method TypeHints and return metadata:

    remove_first = True
    class Class1:
        def method(self,                               {
                   arg1: int,                               'arg1': <Parameter "arg1: int">,
                   arg2: JobArgs,                   ->      'arg2': <Parameter "arg2: JobArgs">,
                   arg3: Union[SomeClass, Transit],         'arg3': <Parameter "arg3: Union[SomeClass, Transit]">,
                   arg4,                                    'arg4': <Parameter "arg4">,
                   *args,                                   'args': <Parameter "*args: str">,
                   **kwargs):                               'kwargs': <Parameter "**kwargs">
            pass                                        }

    remove_first = False                             {
    def function(arg1: int,                              'arg1': <Parameter "arg1: int">,
                 arg2: JobArgs,                          'arg2': <Parameter "arg2: JobArgs">,
                 arg3: Union[SomeClass, Transit]  ->     'arg3': <Parameter "arg3: Union[SomeClass, Transit]">,
                 arg4):                                  'arg4': <Parameter "arg4">
        pass                                         }
    """
    parameters = signature(func).parameters
    if remove_first:
        param = parameters.copy()
        if not param:
            return param
        param.pop(list(param)[0])
        return param
    return parameters.copy()


def initialize_class(stack: str, cls: Type,
                     args: Tuple, kwargs: Dict[str, Any]) -> Any:
    try:
        instance = cls(*args, **kwargs)
    except Exception as exc:
        error_formatter(
            exc, f"Operation: {stack}. An error occurred when trying to initialize class {cls}")
        raise exc
    log.info(f"Operation: {stack}. The class: {cls.__name__} has been successfully initialized.")

    return instance


def call_func_or_method(stack: str, func: Callable,
                        args: Tuple, kwargs: Dict[str, Any]) -> Any:
    try:
        execution_result = func(*args, **kwargs)
    except Exception as exc:
        error_formatter(exc, f"Operation: {stack}. An error occurred while calling entity.")
        raise exc
    return execution_result


def expand_special_kwargs(kwargs: Dict[str, Any], rw_inst: Dict[str, Any]) -> Dict[str, Any]:
    """If kwargs values contains string path to the value stored in the
    dataclass field (with dots division) then replace the string with it, otherwise
    leave the parameter as the same string.

    @dataclass
    class Class1:
        field3: int = 5

    @dataclass
    class JobArgs:
        field1: int = 1
        field2: Class1 = Class1()

    rw_inst = {"ja": JobArgs()}
    kwargs = {"arg1": MandatoryArgTypeContainer("ja.field1")[int],
              "arg2": MandatoryArgTypeContainer("ja.field2.field3")[int],
              "arg3": MandatoryArgTypeContainer("Class1.field3")[int]}

    return_result
        inside type containers:
            {"arg1": MATC().expanded_param = 1,
             "arg2": MATC().expanded_param = 5,
             "arg3": MATC().expanded_param = "The parameter was not expanded."}

    rw_inst = {"ja": JobArgs()}
    kwargs = {"arg1": "ja"}
    return_result = {"arg1": JobArgs()}
    """
    for param_name, param in kwargs.items():
        if is_it_init_arg_type(param) and param.param_link:
            splited_param = param.param_link.split(".")
            result = find_rw_inst(splited_param[0], rw_inst)
            if result:
                if len(splited_param) == 1:
                    param.par_value = result
                    kwargs[param_name] = param
                else:
                    for field in splited_param[1:]:
                        result = result.__getattribute__(field)
                    param.par_value = result
                    kwargs[param_name] = param
            else:
                param.par_value = PARAMETER_WAS_NOT_EXPANDED
                kwargs[param_name] = param

        elif isinstance(param, str):
            if param in rw_inst:
                kwargs[param_name] = rw_inst[param]

    return kwargs


def expand_special_args(args: Tuple, rw_inst: Dict[str, Any]) -> Tuple:
    """If argument contain path to the value stored in the dataclass field then replace the string with it,
    otherwise leave the parameter as the same string containing dots. Example:

    @dataclass
    class BB:
        field3: str = "two"

    @dataclass
    class AA:
        field1: int = 1
        field2: BB = BB()

    rw_inst={"aa": AA()}
    args = (MandatoryArgTypeContainer("aa.field2.field3")[str],
            MandatoryArgTypeContainer("aa.field1")[int],
            "AA",
            "aa.field1",
            "aa",
            MandatoryArgTypeContainer("aa.field_not_exist")[Any])

    return_result
        inside type containers:
            (MATC().expanded_param = "two",
             MATC().expanded_param = 1,
             "AA",
             "aa.field1",
             AA(),
             MATC().expanded_param = "The parameter was not expanded.")
    """
    new_args = []
    for arg in args:
        if is_it_init_arg_type(arg) and arg.param_link:
            splited_arg = arg.param_link.split(".")
            result = find_rw_inst(splited_arg[0], rw_inst)

            if result and len(splited_arg) > 1:
                for field in splited_arg[1:]:
                    result = result.__getattribute__(field)
                arg.par_value = result
                new_args.append(arg)
                continue
            else:
                arg.par_value = PARAMETER_WAS_NOT_EXPANDED
                new_args.append(arg)

        elif isinstance(arg, str):
            if arg in rw_inst:
                new_args.append(rw_inst[arg])
                continue

        new_args.append(arg)

    return tuple(new_args)


def get_instace_from_str(stack_message: str, string: str, rw_inst: Dict[str, Any]) -> Any:
    """Parse string and find instance of executable entity of operation.

    class AA:
        def __init__(self):
            pass
        def method1(self):
            pass
        @staticmethod
        def method2(self):
            pass

    @dataclass
    class BB:
        field_contain_instance: AA = AA()

    @dataclass
    class Transit:
        bb_class: BB = BB()

    string = "t.bb_class.field_contain_instance"
    rw_inst = {"t": Transit()}                        ->  AA()
    """
    splited_str = string.split(".")

    result = find_rw_inst(splited_str[0], rw_inst)
    if not result:
        raise TypeError(f"{stack_message}The string passed as the main executable "
                        f"class of the operation must begin with its alias.")

    for field in splited_str[1: len(splited_str)]:
        result = result.__getattribute__(field)

    return result
