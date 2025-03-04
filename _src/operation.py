from inspect import isfunction, isclass, ismethod, Parameter, signature
from typing import Any, Dict, Optional, Tuple, Union, Callable, Type

from _src.constants import PARAMETER_WAS_NOT_EXPANDED
from _src.default.assign_results import assign
from _src.launch_operations.errors import IncorrectParameterError, AssignmentError, DistributionError
from _src.utils.common import to_tuple
from _src.launch_operations.rw_inst_updater import RwInstUpdater
from _src.initialization_core import InitCore, is_it_init_arg_type
from _src.utils.common import find_rw_inst
from _src.utils.formatters import LoggerBuilder, error_formatter

log = LoggerBuilder().build()


class OpProcessor:
    @staticmethod
    def process_one_operation(
            operation: "Operation",
            input_data: Tuple,
            rw_inst: Dict[str, Any],
            op_stack_name: Optional[str] = None,
            hide_init_inf_from_logs: bool = False,
            check_type_strategy_all: bool = True) -> Tuple[Optional[Any], Optional[Tuple]]:

        OpProcessor._is_it_operation_check(op_stack_name, operation)
        rem_data = None
        internal_init_flag = False

        if operation._obj._function:
            params_wo_self = OpProcessor._get_params_wo_self(
                operation._obj._function, False)
            args, kwargs, rem_data = OpProcessor._get_args_kwargs(
                op_stack_name, *operation._obj._func_args_kwargs,
                params_wo_self, input_data, rw_inst,
                hide_init_inf_from_logs, check_type_strategy_all)
            return OpProcessor._call_func_or_method(
                op_stack_name, operation._obj._function, args, kwargs), rem_data

        elif operation._obj._class:
            params_wo_self = OpProcessor._get_params_wo_self(
                operation._obj._class.__init__) if "__init__" in vars(
                operation._obj._class) else OpProcessor._get_params_wo_self(
                operation._obj._class, False)
            args, kwargs, rem_data = OpProcessor._get_args_kwargs(
                op_stack_name, *operation._obj._init_args_kwargs,
                params_wo_self, input_data, rw_inst,
                hide_init_inf_from_logs, check_type_strategy_all)
            operation._obj._instance = OpProcessor._initialize_class(
                op_stack_name, operation._obj._class, args, kwargs)
            internal_init_flag = True

        if operation._obj._instance and not operation._obj._method:
            return operation._obj._instance, rem_data
        else:
            rem_data = input_data if not internal_init_flag else rem_data
            rem_data = () if rem_data is None else rem_data
            method = operation._obj._instance.__getattribute__(
                operation._obj._method)
            params_wo_self = OpProcessor._get_params_wo_self(
                method.__func__) if ismethod(
                method) else OpProcessor._get_params_wo_self(
                method, remove_first=False)
            args, kwargs, rem_data = OpProcessor._get_args_kwargs(
                op_stack_name, *operation._obj._meth_args_kwargs,
                params_wo_self, rem_data, rw_inst,
                hide_init_inf_from_logs, check_type_strategy_all)

            return OpProcessor._call_func_or_method(
                op_stack_name, method, args, kwargs), rem_data

    @staticmethod
    def _is_it_operation_check(stack: str, operation: Any):
        if not isinstance(operation, Operation):
            raise TypeError(f"Operation: {stack}. Operation must be a class Operation. "
                            f"Passed entity: {operation}")

    @staticmethod
    def _get_args_kwargs(
            op_stack_name: str,
            args: Tuple,
            kwargs: Dict[str, Any],
            params_wo_self: Dict[str, Parameter],
            input_data: Tuple,
            rw_inst: Dict[str, Any],
            hide_init_inf_from_logs: bool,
            check_type_strategy_all: bool = True):
        args = OpProcessor._expand_special_args(
            args, rw_inst)
        kwargs = OpProcessor._expand_special_kwargs(
            kwargs, rw_inst)
        return InitCore.get_args_kwargs(
            op_stack_name, params_wo_self, args, kwargs,
            input_data, hide_init_inf_from_logs, check_type_strategy_all)

    @staticmethod
    def _get_params_wo_self(func: Callable, remove_first: bool = True) -> Dict[str, Parameter]:
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

    @staticmethod
    def _call_func_or_method(
            stack: str, func: Callable,
            args: Tuple, kwargs: Dict[str, Any]) -> Any:
        try:
            execution_result = func(*args, **kwargs)
        except Exception as exc:
            error_formatter(exc, f"Operation: {stack}. An error occurred while calling entity.")
            raise exc
        return execution_result

    @staticmethod
    def _initialize_class(
            stack: str, cls: Type,
            args: Tuple, kwargs: Dict[str, Any]) -> Any:
        try:
            instance = cls(*args, **kwargs)
        except Exception as exc:
            error_formatter(
                exc, f"Operation: {stack}. An error occurred when trying to initialize class {cls}")
            raise exc
        log.info(f"Operation: {stack}. The class: {cls.__name__} has been successfully initialized.")

        return instance

    @staticmethod
    def _expand_special_kwargs(kwargs: Dict[str, Any], rw_inst: Dict[str, Any]) -> Dict[str, Any]:
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
                {"arg1": MATC().par_value = 1,
                 "arg2": MATC().par_value = 5,
                 "arg3": MATC().par_value = "The parameter was not expanded."}

        rw_inst = {"ja": JobArgs()}
        kwargs = {"arg1": MandatoryArgTypeContainer("ja")}
        return_result = {"arg1": JobArgs()}
        """
        for param_name, param in kwargs.items():
            if is_it_init_arg_type(param) and param.param_link:
                splited_arg = param.param_link.split(".")
                result = find_rw_inst(splited_arg[0], rw_inst)
                if result:
                    if len(splited_arg) == 1:
                        param.par_value = result
                        kwargs[param_name] = param
                    else:
                        for field in splited_arg[1:]:
                            result = result.__getattribute__(field)
                        param.par_value = result
                        kwargs[param_name] = param
                else:
                    param.par_value = PARAMETER_WAS_NOT_EXPANDED
                    kwargs[param_name] = param

        return kwargs

    @staticmethod
    def _expand_special_args(args: Tuple, rw_inst: Dict[str, Any]) -> Tuple:
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
                MandatoryArgTypeContainer("aa"),
                MandatoryArgTypeContainer("aa.field_not_exist")[Any])

        return_result
            inside type containers:
                (MATC().par_value = "two",
                 MATC().par_value = 1,
                 "AA",
                 "aa.field1",
                 "aa",
                 MATC().par_value = AA(),
                 MATC().par_value = "The parameter was not expanded.")
        """
        new_args = []
        for arg in args:
            if is_it_init_arg_type(arg) and arg.param_link:
                splited_arg = arg.param_link.split(".")
                result = find_rw_inst(splited_arg[0], rw_inst)
                if result:
                    if len(splited_arg) == 1:
                        arg.par_value = result
                        new_args.append(arg)
                        continue
                    else:
                        for field in splited_arg[1:]:
                            result = result.__getattribute__(field)
                        arg.par_value = result
                        new_args.append(arg)
                        continue
                else:
                    arg.par_value = PARAMETER_WAS_NOT_EXPANDED
                    new_args.append(arg)

            new_args.append(arg)

        return tuple(new_args)


class CallObject:
    def __init__(self,
                 cls_func_inst: Union[Callable, Type, Any]) -> None:
        self._cls_func_inst = cls_func_inst
        self._class = None
        self._function = None
        self._instance = None
        self._method = None
        self._func_args_kwargs = None
        self._init_args_kwargs = None
        self._meth_args_kwargs = None
        self._call_counter: int = 0

    def __call__(self, *args, **kwargs) -> "CallObject":
        if self._method is None:
            if isfunction(self._cls_func_inst):
                self._func_args_kwargs = args, kwargs
                self._function = self._cls_func_inst
            elif isclass(self._cls_func_inst):
                self._init_args_kwargs = args, kwargs
                self._class = self._cls_func_inst
        else:
            if not isfunction(self._cls_func_inst) and \
                    not isclass(self._cls_func_inst):
                self._instance = self._cls_func_inst
            self._meth_args_kwargs = args, kwargs

        self._call_counter += 1
        return self

    def __getattr__(self, name: str) -> "CallObject":
        self._method = name
        return self

    @staticmethod
    def _get_instace_from_str(stack: str, string: str, rw_inst: Dict[str, Any]) -> Any:
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
            existing_aliases = f"{list(rw_inst)}" if rw_inst else f"{rw_inst}"
            raise TypeError(f'Operation: {stack}. No such alias "{splited_str[0]}" '
                            f'in rw_inst. Existing_aliases: {existing_aliases}.')

        class_name = result.__class__.__name__

        for field in splited_str[1: len(splited_str)]:
            try:
                result = result.__getattribute__(field)
            except AttributeError:
                raise AttributeError(
                    f'Operation: {stack}. The RW class "{class_name}" '
                    f'does not have attribute "{field}".')

        return result

    def _get_instance_from_string(self, op_stack_name: str, rw_inst: Dict[str, Any]):
        if isinstance(self._instance, str):
            self._instance = CallObject._get_instace_from_str(
                op_stack_name, self._instance, rw_inst)

    def _get_entity_name(self):
        end_part_name = ""
        if self._class and not self._method:
            end_part_name = "(instance)"
        elif self._method and not self._instance:
            end_part_name = f".{self._method}"
        elif isinstance(self._instance, str) and self._method:
            end_part_name = f'External instance from string: "{self._instance}"'
        elif self._instance and self._method:
            end_part_name = f"(ext_instance).{self._method}"

        if self._function:
            clss_inst_func = self._function.__name__
        elif self._class:
            clss_inst_func = self._class.__name__
        else:
            if isinstance(self._instance, str):
                clss_inst_func = ""
            else:
                clss_inst_func = self._instance.__class__.__name__

        return clss_inst_func + end_part_name



class Operation:
    def __init__(self, call_object: CallObject) -> None:
        self._obj = call_object
        self._op_name: Optional[str] = None
        self._def_args: Optional[Tuple] = None
        self._assign: Optional[Tuple[str]] = None
        self._hide_init_inf_from_logs: Optional[bool] = None
        self._check_type_strategy_all: Optional[bool] = None
        self._distribute_input_data: bool = False
        self._stop_distribution: bool = False
        self._burn_rem_args: bool = False
        self._raise_err_if_empty_data: bool = False

        self._rw_inst: Optional[Dict[str, Any]] = None
        self._rw_inst_from_option: Optional[Dict[str, Any]] = None

        self._branch_stack: Optional[str] = None
        self._operation_stack: Optional[str] = None
        self._last_op_stack: Optional[str] = "INITIAL RUN"

    def op_name(self, name: str) -> "Operation":
        self._op_name = name
        return self

    def rw_inst(self, rw_inst: Dict[str, Any]) -> "Operation":
        self._rw_inst_from_option = rw_inst
        return self

    def def_args(self, *def_args: Any) -> "Operation":
        self._def_args = def_args
        return self

    def assign(self, *args: str) -> "Operation":
        self._assign = args
        return self

    def hide_init_inf_from_logs(self, value: bool) -> "Operation":
        self._hide_init_inf_from_logs = value
        return self

    def check_type_strategy_all(self, value: bool) -> "Branch":
        self._check_type_strategy_all = value
        return self

    @property
    def distribute_input_data(self) -> "Operation":
        self._distribute_input_data = True
        return self

    @property
    def stop_distribution(self) -> "Operation":
        self._stop_distribution = True
        return self

    @property
    def burn_rem_args(self) -> "Operation":
        self._burn_rem_args = True
        return self

    @property
    def raise_err_if_empty_data(self) -> "Operation":
        self._raise_err_if_empty_data = True
        return self

    def _get_op_name(self) -> None:
        if self._op_name is None or self._op_name.startswith(
                "External instance from string"):
            self._op_name = self._obj._get_entity_name()

    def _set_branch_stack(self, stack: str) -> None:
        self._branch_stack = stack

    def _set_last_op_stack(self, stack: str) -> None:
        self._last_op_stack = stack

    def _update_stack(self) -> str:
        self._get_op_name()
        if self._branch_stack is None:
            return self._op_name
        return f"{self._branch_stack} -> {self._op_name}"

    def _get_op_stack(self) -> str:
        return self._operation_stack

    def _check_name(self) -> None:
        OptionsChecker.check_name(self._op_name, self._last_op_stack)

    def _update_rw_inst(self, rw_inst: Dict[str, Any]) -> None:
        self._operation_stack = self._update_stack()
        self._rw_inst = RwInstUpdater.get_updated(
            self._operation_stack, self._rw_inst, rw_inst)
        self._obj._get_instance_from_string(
            self._operation_stack, self._rw_inst)
        self._operation_stack = self._update_stack()

    def run(self, input_data: Optional[Tuple] = None) -> Tuple[Optional[Any], Optional[Tuple]]:
        input_data = () if input_data is None else input_data
        self._check_name()
        self._update_rw_inst(self._rw_inst_from_option)
        OptionsChecker.check_burn_rem_args_op(
            self._operation_stack, self._burn_rem_args,
            self._distribute_input_data)
        OptionsChecker.check_stop_distribution(
            self._operation_stack, self._stop_distribution,
            self._distribute_input_data)

        result, rem_data = OpProcessor.process_one_operation(
            self, input_data, self._rw_inst,
            self._operation_stack, self._hide_init_inf_from_logs,
            self._check_type_strategy_all)

        if self._burn_rem_args:
            rem_data = None

        if self._assign is not None:
            return Assigner.do_assign(
                self._operation_stack, self._assign,
                self._rw_inst, result), None

        return result, rem_data


class OptionsChecker:
    @staticmethod
    def check_name(name: Optional[str], last_op_stack: str) -> None:
        if name is not None and not isinstance(name, str):
            raise IncorrectParameterError(
                f"The last successful operation: {last_op_stack}. "
                f"The name passed in the option "
                f"must be in string format.")

    @staticmethod
    def check_assign_option(
            stack: str,
            fields_for_assign: Optional[Tuple[str, ...]],
            rw_inst: Dict[str, Any]) -> None:
        if fields_for_assign is not None:
            if not all(map(lambda x: isinstance(x, str), fields_for_assign)):
                raise TypeError(
                    f"Operation: {stack}. All values to assign must be string only.")
            aliases = list(map(lambda x: x.split(".")[0], fields_for_assign))
            for alias in aliases:
                if alias not in rw_inst:
                    raise AssignmentError(
                        f"Operation: {stack}. Alias \"{alias}\" "
                        f"is missing from rw_inst. Assignment not possible.")
            fields = list(map(lambda x: x.split(".")[1:], fields_for_assign))
            for fields_list in fields:
                for field in fields_list:
                    if not field.isidentifier():
                        raise AssignmentError(
                            f'Operation: {stack}.\nPart of string reference to '
                            f'an object "{field}" cannot be a python field.')

    @staticmethod
    def check_burn_rem_args_op(
            stack: str, burn_rem_args: bool,
            distribute_input_data: bool) -> None:
        if burn_rem_args and distribute_input_data:
            raise DistributionError(
                f"Operation: {stack}.\nIt is not possible to simultaneously\n"
                f"burn the remaining arguments and distribute the data.\n"
                f"Because distribution use remaining args.")

    @staticmethod
    def check_burn_rem_args_br(
            stack: str,
            burn_rem_args: bool,
            stop_distribution: bool,
            delayed_return: Optional[Tuple]) -> None:
        if burn_rem_args and delayed_return is not None and not stop_distribution:
            raise DistributionError(
                f"Operation: {stack}.\nIt is not possible to simultaneously\n"
                f"burn the remaining arguments and distribute the data.\n"
                f"Because distribution use remaining args.")


    @staticmethod
    def check_stop_distribution(
            stack: str, stop_distribution: bool,
            distribute_input_data: bool) -> None:
        if stop_distribution and distribute_input_data:
            raise DistributionError(
                f"Operation: {stack}. It is not possible to simultaneously\n"
                f"start and stop distribution")


class Assigner:
    @staticmethod
    def do_assign(
            stack: str,
            fields_for_assign: Tuple[str, ...],
            rw_inst: Dict[str, Any],
            result: Optional[Any]):
        OptionsChecker.check_assign_option(stack, fields_for_assign, rw_inst)
        Assigner._validate_result(stack, result, fields_for_assign)
        kw = {key: rw_inst[key.split(".")[0]] for key in fields_for_assign}
        return assign(*to_tuple(result), **kw)

    @staticmethod
    def _validate_result(
            stack: str,
            result: Optional[Any],
            fields_for_assign: Tuple[str, ...]) -> None:
        if result is None:
            raise AssignmentError(
                f"Operation: {stack}. The result of the operation is None. "
                f"Assignment is not possible.")
        len_result = len(to_tuple(result))
        if len_result != len(fields_for_assign):
            raise AssignmentError(
                f"Operation: {stack}. The number of positional arguments after "
                f"the operation execution is {len_result} and it is not equal to "
                f"the number of fields to assign, they were found {len(fields_for_assign)}")
