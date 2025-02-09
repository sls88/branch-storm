from inspect import Parameter, signature, ismethod, isfunction, isclass
from typing import Any, Dict, Optional, Tuple, Callable, Type, Union

from src.processor.initialization import get_args_kwargs
from src.operation_branch import Operation
from src.utils.formatters import error_formatter, LoggerBuilder


log = LoggerBuilder().build()


def process_one_operation(
        operation: Union[Operation, Callable, Type],
        init_data: Tuple,
        rw_inst: Dict[str, Any],
        name_stack: Optional[str] = None,
        op_name: Optional[str] = None) -> Tuple[Optional[Any], str, Optional[Tuple]]:

    # print(8888888888, operation, init_data, rw_inst, name_stack, op_name)
    stack_message = get_message(name_stack, op_name)
    if not any([isinstance(operation, Operation), isfunction(operation), isclass(operation)]):
        raise TypeError(f"{stack_message}Operation must be a class Operation, "
                        f"function or class. Passed entity: {operation}")

    if isfunction(operation):
        operation = Operation(operation).func()
    elif isclass(operation):
        operation = Operation(operation).init().meth()

    if not operation._func_args_kwargs and not operation._init_args_kwargs and not operation._meth_args_kwargs:
        operation = operation.func()

    instance, rem_data = (operation._instance, None) if operation._instance else (None, None)
    if isinstance(instance, str):
        operation._instance = get_instace_from_str(stack_message, instance, rw_inst)

    method_name = get_method_name(operation, stack_message)
    stack = get_object_name(operation, method_name, name_stack, op_name)

    if operation._func_args_kwargs:
        params_wo_self = {name: par for name, par in get_params_wo_self(
            operation._class_or_func, False).items()}
        args, kwargs, rem_data = get_args_kwargs(params_wo_self, *operation._func_args_kwargs, init_data)
        kwargs = expand_special_kwargs(kwargs, rw_inst)
        args = expand_special_args(args, rw_inst)
        return call_func_or_method(stack, operation._class_or_func, args, kwargs), stack, rem_data
    elif operation._init_args_kwargs:
        params_wo_self = {name: par for name, par in get_params_wo_self(
            operation._class_or_func, True).items()}
        args, kwargs, rem_data = get_args_kwargs(params_wo_self, *operation._init_args_kwargs, init_data)
        kwargs = expand_special_kwargs(kwargs, rw_inst)
        args = expand_special_args(args, rw_inst)
        instance = initialize_class(stack, operation._class_or_func, args, kwargs)

    if instance and not operation._meth_args_kwargs:
        return instance, stack, rem_data
    else:
        method = instance.__getattribute__(method_name)
        params_wo_self = get_params_wo_self(method.__func__) if ismethod(method) \
            else get_params_wo_self(method, remove_first=False)
        args, kwargs, rem_data = get_args_kwargs(params_wo_self, *operation._func_args_kwargs, init_data)
        kwargs = expand_special_kwargs(kwargs, rw_inst)
        args = expand_special_args(args, rw_inst)
        return call_func_or_method(stack, operation._class_or_func, args, kwargs), stack, rem_data


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
        param.pop(list(param)[0])
        return param
    return parameters.copy()


def get_message(name_stack: Optional[str], obj_name: Optional[str]) -> str:
    if name_stack is None and obj_name is None:
        return ""
    elif name_stack is None:
        return f"Call stack: {obj_name}. "
    elif obj_name is None:
        return f"Call stack: {name_stack}. "
    return f"Call stack: {name_stack} -> {obj_name}. "


def initialize_class(obj_name: str, cls: Type,
                     args: Tuple, kwargs: Dict[str, Any]) -> Any:
    try:
        instance = cls(*args, **kwargs)
    except Exception as exc:
        error_formatter(
            exc, f"Call stack: {obj_name}. An error occurred when trying to initialize class {cls}")
        raise exc
    log.info(f"Call stack: {obj_name}. The class: {cls.__name__} has been successfully initialized.")

    return instance


def find_method_to_run(stack_message: str, class_type: Type) -> str:
    """Find and return the name of the runnable class method_name.

    If class has 0 or more than 1 runnable methods throw ValueError.
    """
    all_class_methods = dir(class_type)
    method_to_run = list(
        filter(lambda x: not (x.startswith("_") or x.startswith("__")), all_class_methods))
    len_methods = len(method_to_run)
    if len_methods != 1:
        raise TypeError(
            f"{stack_message}Object type {class_type} has {len_methods} runnable methods. "
            f"The name of a runnable method_name must not begin with '_' or '__' and there must "
            f"be only one in the class.{f' Find methods {method_to_run}' if method_to_run else ''}")
    return method_to_run[0]


def call_func_or_method(obj_name: str, func: Callable,
                        args: Tuple, kwargs: Dict[str, Any]) -> Any:
    try:
        execution_result = func(*args, **kwargs)
    except Exception as exc:
        error_formatter(exc, f"Call stack: {obj_name}. An error occurred while calling entity.")
        raise exc
    return execution_result


def find_rw_inst(string: str, rw_inst: Dict[str, Any]) -> Optional[Type]:
    """Return a special class if the string parameter is equal its alias.

    rw_inst = {"ja": JobArgs, "ac": AnotherClass}
    string = "ja" -> JobArgs()

    rw_inst = {"ac": AnotherClass}
    string = "ja" -> None
    """
    for alias in rw_inst:
        if string == alias:
            return rw_inst[alias]


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
    kwargs = {"arg1": "ja.field1", "arg2": "ja.field2.field3", "arg3": "Class1.field3"}
    return_result = {"arg1": 1, "arg2": 5, "arg3": "Class1.field3"}

    rw_inst = {"arg1": JobArgs()}
    kwargs = {"arg1": "ja"}
    return_result = {"arg1": JobArgs()}
    """
    for par_name, par in kwargs.items():
        if isinstance(par, str):
            splited_par = par.split(".")
            result = find_rw_inst(splited_par[0], rw_inst)
            if result:
                if len(splited_par) == 1:
                    kwargs[par_name] = result
                else:
                    for field in splited_par[1:]:
                        result = result.__getattribute__(field)

                    kwargs[par_name] = result
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
    args = ("aa.field2.field3", "aa.field1", "AA", "aa", "aa.field3")

    return_result = ("two", 1, "AA", "aa", "aa.field3")
    """
    new_args = []
    for par in args:
        if isinstance(par, str):
            splited_par = par.split(".")
            result = find_rw_inst(splited_par[0], rw_inst)

            if result and len(splited_par) > 1:
                for field in splited_par[1:]:
                    result = result.__getattribute__(field)
                new_args.append(result)
                continue
        new_args.append(par)

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


def get_method_name(operation: Operation, stack_message: str) -> Optional[str]:
    if not operation._func_args_kwargs:
        clss = type(operation._instance) if operation._instance else operation._class_or_func
        return operation._method if operation._method else find_method_to_run(stack_message, clss)
    return None


def get_object_name(operation: Operation,
                    method_name: Optional[str],
                    name_stack: Optional[str],
                    op_name: Optional[str]) -> str:
    end_part_name = ""
    if operation._init_args_kwargs and not operation._meth_args_kwargs:
        end_part_name = "(instance)"
    elif method_name:
        end_part_name = f".{method_name}"

    if name_stack is None and op_name is None:
        return operation._class_or_func.__name__ + end_part_name
    elif name_stack is None:
        return op_name
    elif op_name is None:
        op_name = operation._class_or_func.__name__ + end_part_name
        return f"{name_stack} -> {op_name}"
    return f"{name_stack} -> {op_name}"
