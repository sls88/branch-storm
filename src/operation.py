from inspect import isfunction, isclass, ismethod
from typing import Any, Dict, Optional, Tuple, Union, Callable, Type

from src.default.assign_results import assign
from src.launch_operations.launch_utils import to_tuple
from src.launch_operations.rw_inst_updater import RwInstUpdater
from src.processor.initialization import get_args_kwargs
from src.processor.one_op_processor import call_func_or_method, expand_special_args, expand_special_kwargs, \
    get_params_wo_self, initialize_class
from src.utils.common import find_rw_inst


def process_one_operation(
        operation: "Operation",
        input_data: Tuple,
        rw_inst: Dict[str, Any],
        op_stack_name: Optional[str] = None,
        hide_init_inf_from_logs: bool = False) -> Tuple[Optional[Any], Optional[Tuple]]:

    # print(8888888888, operation, init_data, rw_inst, name_stack, op_name)
    # print(88888, operation._obj._func_args_kwargs, operation._obj._init_args_kwargs, operation._obj._meth_args_kwargs,
    #       operation._obj._class, operation._obj._instance, rw_inst, op_stack_name, init_data)
    # stack_message = get_message(name_stack, op_stack_name)
    if not isinstance(operation, Operation):
        raise TypeError(f"Stack: {op_stack_name}. Operation must be a class Operation. "
                        f"Passed entity: {operation}")

    rem_data = None
    internal_init_flag = False

    # stack = get_object_name(operation, name_stack, op_stack_name)
    if operation._obj._function:
        params_wo_self = get_params_wo_self(operation._obj._function, False)
        f_args, f_kwargs = operation._obj._func_args_kwargs
        args, kwargs, rem_data = get_args_kwargs(
            op_stack_name, params_wo_self, f_args, f_kwargs,
            input_data, hide_init_inf_from_logs)
        kwargs = expand_special_kwargs(kwargs, rw_inst)
        args = expand_special_args(args, rw_inst)
        return call_func_or_method(op_stack_name, operation._obj._function, args, kwargs), rem_data
    elif operation._obj._class:
        params_wo_self = get_params_wo_self(operation._obj._class.__init__) if \
            "__init__" in vars(operation._obj._class) else \
            get_params_wo_self(operation._obj._class, False)
        i_args, i_kwargs = operation._obj._init_args_kwargs
        args, kwargs, rem_data = get_args_kwargs(
            op_stack_name, params_wo_self, i_args, i_kwargs,
            input_data, hide_init_inf_from_logs)
        kwargs = expand_special_kwargs(kwargs, rw_inst)
        args = expand_special_args(args, rw_inst)
        operation._obj._instance = initialize_class(op_stack_name, operation._obj._class, args, kwargs)
        internal_init_flag = True

    if operation._obj._instance and not operation._obj._method:
        return operation._obj._instance, rem_data
    else:
        rem_data = input_data if not internal_init_flag else rem_data
        method = operation._obj._instance.__getattribute__(operation._obj._method)
        params_wo_self = get_params_wo_self(method.__func__) if ismethod(method) \
            else get_params_wo_self(method, remove_first=False)
        m_args, m_kwargs = operation._obj._meth_args_kwargs
        args, kwargs, rem_data = get_args_kwargs(
            op_stack_name, params_wo_self, m_args, m_kwargs,
            rem_data, hide_init_inf_from_logs)
        kwargs = expand_special_kwargs(kwargs, rw_inst)
        args = expand_special_args(args, rw_inst)

        return call_func_or_method(op_stack_name, method, args, kwargs), rem_data


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
        self._distribute_input_data: bool = False
        self._stop_distribution: bool = False
        self._burn_rem_args: bool = False
        self._raise_err_if_empty_data = False

        self._rw_inst: Optional[Dict[str, Any]] = None
        self._rw_inst_from_option: Optional[Dict[str, Any]] = None

        self._branch_stack: Optional[str] = None
        self._operation_stack: Optional[str] = None

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

    def _update_stack(self) -> str:
        self._get_op_name()
        if self._branch_stack is None:
            return self._op_name
        return f"{self._branch_stack} -> {self._op_name}"

    def _get_op_stack(self) -> str:
        return self._operation_stack

    def _update_rw_inst(self, rw_inst: Dict[str, Any]) -> None:
        self._operation_stack = self._update_stack()
        self._rw_inst = RwInstUpdater.get_updated(
            self._operation_stack, self._rw_inst, rw_inst)
        self._obj._get_instance_from_string(self._operation_stack, self._rw_inst)
        self._operation_stack = self._update_stack()

    def run(self, input_data: Tuple) -> Tuple[Optional[Any], Optional[Tuple]]:
        self._update_rw_inst(self._rw_inst_from_option)

        result, rem_data =  process_one_operation(
            self, input_data, self._rw_inst,
            self._operation_stack, self._hide_init_inf_from_logs)

        if self._assign is not None:
            kw = {key: self._rw_inst[key.split(".")[0]] for key in self._assign}
            return assign(*to_tuple(result), **kw), None

        return result, rem_data
