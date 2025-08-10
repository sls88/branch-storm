from dataclasses import dataclass, replace, field
from enum import Enum
from inspect import isfunction, isclass, ismethod, Parameter, signature
from typing import Any, Dict, Optional, Tuple, Union, Callable, Type

from .constants import PARAMETER_WAS_NOT_EXPANDED, SINGLE_RUN
from .default.assign_results import assign
from .default.rw_classes import RunConfigurations, RwInstUpdater
from .launch_operations.errors import AssignmentError
from .utils.common import to_tuple
from .utils.options_utils import OptionsChecker
from .initialization_core import InitCore, is_it_init_arg_type, is_it_arg_type
from .utils.common import find_rw_inst
from .utils.formatters import LoggerBuilder, error_formatter

log = LoggerBuilder().build()

Cond = Callable[[Optional[Any]], bool]
ArgTuple   = Tuple[Any, ...]
KwargsDict = Dict[str, Any]
ParamsMap  = Dict[str, Parameter]
HideLogInf = Tuple[bool, bool]


class _TargetKind(Enum):
    FUNC = "func"
    CLASS_INIT = "class_init"
    INSTANCE_ONLY = "instance_only"
    BOUND_METHOD = "bound_method"


@dataclass
class _CallArgs:
    args: ArgTuple
    kwargs: KwargsDict


@dataclass
class _Target:
    kind: _TargetKind
    func: Optional[Callable] = None
    cls: Optional[Type] = None
    instance: Optional[Any] = None
    call_args: Optional[_CallArgs] = None


class OpBuilder:
    @staticmethod
    def build_and_call(
        operation: "Operation",
        input_data: ArgTuple,
        rw_inst: Dict[str, Any],
        op_stack_name: Optional[str] = None,
        hide_log_inf: HideLogInf = (False, False),
        check_type_strategy_all: bool = True
    ) -> Tuple[Optional[Any], Optional[ArgTuple]]:

        OpBuilder._is_it_operation_check(op_stack_name, operation)
        obj = operation._obj
        target = OpBuilder._detect_target(obj)
        return OpBuilder._dispatch(
            target, obj, input_data, rw_inst, op_stack_name,
            hide_log_inf, check_type_strategy_all
        )

    @staticmethod
    def _dispatch(
        target: _Target,
        obj: "CallObject",
        input_data: ArgTuple,
        rw_inst: Dict[str, Any],
        stack: Optional[str],
        hide_log_inf: HideLogInf,
        check_type_strategy_all: bool
    ) -> Tuple[Optional[Any], Optional[ArgTuple]]:

        if target.kind is _TargetKind.FUNC:
            return OpBuilder._handle_function(
                target, input_data, rw_inst, stack,
                hide_log_inf, check_type_strategy_all
            )

        if target.kind is _TargetKind.CLASS_INIT:
            instance, rem_from_init = OpBuilder._handle_class_init(
                target, obj, input_data, rw_inst, stack,
                hide_log_inf, check_type_strategy_all)

            if obj._instance and not obj._method:
                return instance, rem_from_init

            bound_target = OpBuilder._make_bound_target(obj)
            return OpBuilder._handle_bound_method(
                bound_target, rw_inst, stack, hide_log_inf, check_type_strategy_all,
                internal_init_flag=True, rem_from_init=rem_from_init, input_data=input_data)

        if target.kind is _TargetKind.INSTANCE_ONLY:
            return obj._instance, None

        return OpBuilder._handle_bound_method(
            target, rw_inst, stack, hide_log_inf, check_type_strategy_all,
            internal_init_flag=False, rem_from_init=None, input_data=input_data)

    @staticmethod
    def _handle_function(
        target: _Target,
        input_data: ArgTuple,
        rw_inst: Dict[str, Any],
        stack: Optional[str],
        hide_log_inf: HideLogInf,
        check_type_strategy_all: bool
    ) -> Tuple[Optional[Any], Optional[ArgTuple]]:
        params = OpBuilder._get_params_wo_self(target.func, remove_first=False)
        cargs = OpBuilder._norm_call_args(target.call_args)
        args, kwargs, rem = OpBuilder._prepare_args_kwargs(
            stack, cargs, params, input_data, rw_inst,
            hide_log_inf, check_type_strategy_all
        )
        result = OpBuilder._call_func_or_method(stack, target.func, args, kwargs)
        return result, rem

    @staticmethod
    def _handle_class_init(
        target: _Target,
        obj: "CallObject",
        input_data: ArgTuple,
        rw_inst: Dict[str, Any],
        stack: Optional[str],
        hide_log_inf: HideLogInf,
        check_type_strategy_all: bool
    ) -> Tuple[Any, Optional[ArgTuple]]:
        params = (
            OpBuilder._get_params_wo_self(target.cls.__init__)
            if "__init__" in vars(target.cls)
            else OpBuilder._get_params_wo_self(target.cls, remove_first=False)
        )
        cargs = OpBuilder._norm_call_args(target.call_args)
        args, kwargs, rem = OpBuilder._prepare_args_kwargs(
            stack, cargs, params, input_data, rw_inst,
            hide_log_inf, check_type_strategy_all
        )
        obj._instance = OpBuilder._initialize_class(stack, target.cls, args, kwargs)
        return obj._instance, rem

    @staticmethod
    def _handle_bound_method(
        target: _Target,
        rw_inst: Dict[str, Any],
        stack: Optional[str],
        hide_log_inf: HideLogInf,
        check_type_strategy_all: bool,
        *,
        internal_init_flag: bool,
        rem_from_init: Optional[ArgTuple],
        input_data: ArgTuple
    ) -> Tuple[Optional[Any], Optional[ArgTuple]]:

        method = target.func
        params = OpBuilder._get_method_params(method)
        eff_rem: ArgTuple = input_data if not internal_init_flag else (rem_from_init or ())
        call_args = OpBuilder._norm_call_args(target.call_args)
        args, kwargs, rem = OpBuilder._prepare_args_kwargs(
            stack, call_args, params, eff_rem, rw_inst,
            hide_log_inf, check_type_strategy_all
        )
        result = OpBuilder._call_func_or_method(stack, method, args, kwargs)
        return result, rem

    @staticmethod
    def _detect_target(obj: "CallObject") -> _Target:
        if obj._function:
            return _Target(
                _TargetKind.FUNC,
                func=obj._function,
                call_args=_CallArgs(*(obj._func_args_kwargs or ((), {})))
            )
        if obj._class:
            return _Target(
                _TargetKind.CLASS_INIT,
                cls=obj._class,
                call_args=_CallArgs(*(obj._init_args_kwargs or ((), {})))
            )
        if obj._instance and not obj._method:
            return _Target(_TargetKind.INSTANCE_ONLY, instance=obj._instance)

        method = getattr(obj._instance, obj._method)
        return _Target(
            _TargetKind.BOUND_METHOD,
            func=method,
            instance=obj._instance,
            call_args=_CallArgs(*(obj._meth_args_kwargs or ((), {})))
        )

    @staticmethod
    def _make_bound_target(obj: "CallObject") -> _Target:
        method = getattr(obj._instance, obj._method)
        return _Target(
            _TargetKind.BOUND_METHOD,
            func=method,
            instance=obj._instance,
            call_args=_CallArgs(*(obj._meth_args_kwargs or ((), {})))
        )

    @staticmethod
    def _norm_call_args(call_args: Optional[_CallArgs]) -> _CallArgs:
        return call_args or _CallArgs((), {})

    @staticmethod
    def _get_method_params(method: Callable) -> ParamsMap:
        if ismethod(method):
            return OpBuilder._get_params_wo_self(method.__func__)
        return OpBuilder._get_params_wo_self(method, remove_first=False)

    @staticmethod
    def _prepare_args_kwargs(
        op_stack_name: str,
        call_args: _CallArgs,
        params_wo_self: ParamsMap,
        input_data: ArgTuple,
        rw_inst: Dict[str, Any],
        hide_log_inf: HideLogInf,
        check_type_strategy_all: bool
    ) -> Tuple[ArgTuple, KwargsDict, Optional[ArgTuple]]:
        args = OpBuilder._expand_special_args(call_args.args, rw_inst)
        kwargs = OpBuilder._expand_special_kwargs(call_args.kwargs, rw_inst)
        return InitCore.get_args_kwargs(
            op_stack_name, params_wo_self, args, kwargs,
            input_data, hide_log_inf, check_type_strategy_all
        )

    @staticmethod
    def _is_it_operation_check(stack: str, operation: Any):
        if not isinstance(operation, Operation):
            raise TypeError(
                f"Operation: {stack}. Operation must be a class Operation. "
                f"Passed entity: {operation}"
            )

    @staticmethod
    def _get_params_wo_self(func: Callable, remove_first: bool = True) -> ParamsMap:
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
    def _call_func_or_method(stack: str, func: Callable,
                             args: ArgTuple, kwargs: KwargsDict) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            error_formatter(
                exc, f"Operation: {stack}. "
                     f"An error occurred while calling entity.")
            raise exc

    @staticmethod
    def _initialize_class(stack: str, cls: Type,
                          args: ArgTuple, kwargs: KwargsDict) -> Any:
        try:
            instance = cls(*args, **kwargs)
        except Exception as exc:
            error_formatter(
                exc, f"Operation: {stack}. "
                     f"An error occurred when trying to initialize class {cls}"
            )
            raise exc
        log.info(f"Operation: {stack}. The class: "
                 f"{cls.__name__} has been successfully initialized.")
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
                            result = getattr(result, field)
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
                            result = getattr(result, field)
                        arg.par_value = result
                        new_args.append(arg)
                        continue
                else:
                    arg.par_value = PARAMETER_WAS_NOT_EXPANDED
                    new_args.append(arg)
            new_args.append(arg)
        return tuple(new_args)


@dataclass
class _Args:
    args: Tuple[Any, ...] = ()
    kwargs: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_maybe(maybe: Optional[Tuple[Tuple[Any, ...], Dict[str, Any]]]) -> "_Args":
        return _Args(*maybe) if maybe is not None else _Args()


class CallObject:
    def __init__(self, cls_func_inst: Union[Callable, Type, Any]) -> None:
        self._cls_func_inst = cls_func_inst

        self._class: Optional[Type] = None
        self._function: Optional[Callable] = None
        self._instance: Optional[Any] = None
        self._method: Optional[str] = None

        self._func_args_kwargs: Optional[Tuple[Tuple[Any, ...], Dict[str, Any]]] = None
        self._init_args_kwargs: Optional[Tuple[Tuple[Any, ...], Dict[str, Any]]] = None
        self._meth_args_kwargs: Optional[Tuple[Tuple[Any, ...], Dict[str, Any]]] = None

        self._call_counter: int = 0

    def __call__(self, *args, **kwargs) -> "CallObject":
        if self._method is None:
            if isfunction(self._cls_func_inst):
                self._function = self._cls_func_inst
                self._func_args_kwargs = (args, kwargs)
            elif isclass(self._cls_func_inst):
                self._class = self._cls_func_inst
                self._init_args_kwargs = (args, kwargs)
        else:
            if not isfunction(self._cls_func_inst) and not isclass(self._cls_func_inst):
                self._instance = self._cls_func_inst
            self._meth_args_kwargs = (args, kwargs)

        self._call_counter += 1
        return self

    def __getattr__(self, name: str) -> "CallObject":
        self._method = name
        return self

    def _try_to_find_slot_for_arg(self) -> bool:
        func = _Args.from_maybe(self._func_args_kwargs)
        init = _Args.from_maybe(self._init_args_kwargs)
        meth = _Args.from_maybe(self._meth_args_kwargs)

        all_args = (
            *func.args, *func.kwargs.values(),
            *init.args, *init.kwargs.values(),
            *meth.args, *meth.kwargs.values(),
        )
        for arg in all_args:
            init_arg_type = is_it_init_arg_type(arg)
            arg_type = is_it_arg_type(arg)
            if init_arg_type == "optional" or arg_type == "optional" or (
                    init_arg_type and arg.param_link is not None):
                continue
            elif is_it_arg_type(arg):
                return True
        return False

    @staticmethod
    def _get_instance_from_str(stack: str, string: str,
                               rw_inst: Dict[str, Any]) -> Any:
        """Parse string and find instance of executable entity of operation."""
        splited_str = string.split(".")

        result = find_rw_inst(splited_str[0], rw_inst)
        if not result:
            existing_aliases = f"{list(rw_inst)}" if rw_inst else f"{rw_inst}"
            raise TypeError(
                f'Operation: {stack}. No such alias "{splited_str[0]}" '
                f'in rw_inst. Existing_aliases: {existing_aliases}.'
            )

        class_name = result.__class__.__name__

        for field in splited_str[1:len(splited_str)]:
            try:
                result = getattr(result, field)
            except AttributeError:
                raise AttributeError(
                    f'Operation: {stack}. The RW class "{class_name}" '
                    f'does not have attribute "{field}".'
                )

        return result

    def _get_instance_from_string(self, op_stack_name: str,
                                  rw_inst: Dict[str, Any]) -> None:
        if isinstance(self._instance, str):
            self._instance = CallObject._get_instance_from_str(
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


@dataclass
class OperationOptions:
    op_name: Optional[str] = None
    rw_inst: Tuple[Dict[str, Any], ...] = ()
    end_chain_cond: Optional[Cond] = None
    raise_err_cond: Optional[Cond] = None
    hide_log_inf: Tuple[Optional[bool], Optional[bool]] = (None, None)
    check_type_strategy_all: Optional[bool] = None
    distribute_input_data: bool = False
    stop_distribution: bool = False
    burn_rem_args: bool = False
    force_call: bool = False
    assign: Optional[Tuple[str, ...]] = None


class BaseOperationMethods:
    def __init__(self, call_object: Optional[CallObject] = None) -> None:
        self._obj: Optional[CallObject] = call_object
        self._opts: OperationOptions = OperationOptions()
        self._run_conf: Optional[RunConfigurations] = None

    def _set_run_conf(self, run_conf: RunConfigurations) -> None:
        self._run_conf = run_conf

    def _check_name(self, last_op_stack: str) -> None:
        OptionsChecker.check_name(self._opts.op_name, last_op_stack)

    def _get_op_name(self, stack: str) -> None:
        self._check_name(stack)
        if self._opts.op_name is None or (
            isinstance(self._opts.op_name, str)
            and self._opts.op_name.startswith("External instance from string")
        ):
            self._opts = replace(self._opts, op_name=self._obj._get_entity_name())

    def _update_stack(self, run_conf: RunConfigurations) -> RunConfigurations:
        self._get_op_name(run_conf.last_op_stack)
        run_conf.set_operation_stack(self._opts.op_name)
        return run_conf

    def _update_rw_inst(self, run_conf: RunConfigurations) -> RunConfigurations:
        run_conf.update_last_rw_inst(self._opts.rw_inst)
        self._obj._get_instance_from_string(run_conf.operation_stack, run_conf.get_rw_inst())
        run_conf = self._update_stack(run_conf)
        run_conf.update_last_rw_inst((run_conf.get_rw_inst(),))
        return run_conf

    def _pull_options(self) -> Tuple[Optional[Cond], Optional[Cond], bool]:
        return self._opts.end_chain_cond, self._opts.raise_err_cond, self._opts.force_call

    def _can_an_op_take_an_arg(self) -> bool:
        return self._obj._try_to_find_slot_for_arg()

    def _get_run_options(
        self, run_conf: RunConfigurations
    ) -> Tuple[Tuple[Optional[bool], Optional[bool]], bool, Dict[str, Any]]:
        curr_init_inf, curr_all_inf = self._opts.hide_log_inf
        opt_init_inf, opt_all_inf = run_conf.br_opt.hide_log_inf
        hide_log_inf = (
            opt_init_inf if curr_init_inf is None else curr_init_inf,
            opt_all_inf if curr_all_inf is None else curr_all_inf,
        )

        curr_check = self._opts.check_type_strategy_all
        check_type_strategy_all = run_conf.br_opt.check_type_strategy_all if curr_check is None else curr_check
        return hide_log_inf, check_type_strategy_all, run_conf.get_rw_inst()

    def op_name(self, name: str) -> "BaseOperationMethods":
        self._opts = replace(self._opts, op_name=name)
        return self

    def rw_inst(self, rw_inst: Dict[str, Any]) -> "BaseOperationMethods":
        self._opts = replace(self._opts, rw_inst=self._opts.rw_inst + (rw_inst,))
        return self

    def end_chain_if(self, condition_func: Cond) -> "BaseOperationMethods":
        self._opts = replace(self._opts, end_chain_cond=condition_func)
        return self

    def raise_err_if(self, condition_func: Cond) -> "BaseOperationMethods":
        self._opts = replace(self._opts, raise_err_cond=condition_func)
        return self

    def assign(self, *args: str) -> "BaseOperationMethods":
        self._opts = replace(self._opts, assign=tuple(args) if args else None)
        return self

    def hide_log_inf(self, init_inf: bool = None, all_inf: bool = None) -> "BaseOperationMethods":
        self._opts = replace(self._opts, hide_log_inf=(init_inf, all_inf))
        return self

    def check_type_strategy_all(self, value: bool) -> "BaseOperationMethods":
        self._opts = replace(self._opts, check_type_strategy_all=value)
        return self

    @property
    def distribute_input_data(self) -> "BaseOperationMethods":
        self._opts = replace(self._opts, distribute_input_data=True)
        return self

    @property
    def stop_distribution(self) -> "BaseOperationMethods":
        self._opts = replace(self._opts, stop_distribution=True)
        return self

    @property
    def burn_rem_args(self) -> "BaseOperationMethods":
        self._opts = replace(self._opts, burn_rem_args=True)
        return self

    @property
    def force_call(self) -> "BaseOperationMethods":
        self._opts = replace(self._opts, force_call=True)
        return self

    @staticmethod
    def _check_passed_conditions(
            op_stack: str, end_chain_cond: Optional[Cond],
            raise_err_cond: Optional[Cond]) -> None:
        def check_callable(op_stack: str, name: str, condition: Optional[Callable]) -> None:
            if condition is not None and not callable(condition):
                raise TypeError(
                    f"Operation: {op_stack}.\n"
                    f"{name} must be callable (e.g., function, lambda)"
                )
        check_callable(op_stack, "end_chain_if", end_chain_cond)
        check_callable(op_stack, "raise_err_if", raise_err_cond)

    @staticmethod
    def _get_end_conditions_flags(
        end_chain_cond: Optional[Cond],
        raise_err_cond: Optional[Cond],
        input_data: Optional[Any] = None
    ) -> Tuple[bool, bool]:
        end_chain_flag = bool(end_chain_cond(input_data)) if end_chain_cond is not None else False
        raise_err_flag = bool(raise_err_cond(input_data)) if raise_err_cond is not None else False
        return end_chain_flag, raise_err_flag


class Operation(BaseOperationMethods):
    def __init__(self, call_object: CallObject) -> None:
        super().__init__(call_object)

    def _pull_options(self) -> Tuple[Optional[Cond], Optional[Cond], bool]:
        return super()._pull_options()

    def run(self, input_data: Optional[Tuple] = None
            ) -> Tuple[Optional[Any], Optional[Tuple]]:
        if input_data is None and not self._can_an_op_take_an_arg():
            input_data = ()

        if not self._run_conf:
            self._get_op_name(SINGLE_RUN)
            op_stack = self._opts.op_name
            OptionsChecker.check_burn_rem_args_op(
                op_stack, self._opts.burn_rem_args, self._opts.distribute_input_data)
            hide_log_inf = self._opts.hide_log_inf
            check_type_strategy_all = self._opts.check_type_strategy_all
            rw_inst = RwInstUpdater.get_updated_all(
                op_stack, None, self._opts.rw_inst)
            self._obj._get_instance_from_string(op_stack, rw_inst)
            self._get_op_name(SINGLE_RUN)
        else:
            hide_log_inf, check_type_strategy_all, rw_inst = self._get_run_options(
                self._run_conf)
            op_stack = self._run_conf.operation_stack

        result, rem_args = OpBuilder.build_and_call(
            self,
            to_tuple(input_data),
            rw_inst,
            op_stack,
            hide_log_inf,
            check_type_strategy_all)

        if self._opts.burn_rem_args:
            rem_args = None

        if self._opts.assign is not None:
            result = do_assign_result(op_stack, self._opts.assign, result, rw_inst)
            rem_args = None

        return result, rem_args


def do_assign_result(
        op_stack: str,
        assign: Optional[Tuple[str, ...]],
        result: Any,
        rw_inst: Dict[str, Any]) -> Any:
    if assign is not None:
        result = Assigner.do_assign(
            op_stack, assign,
            rw_inst, result)
    return result


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
