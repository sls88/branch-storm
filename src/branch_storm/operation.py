import inspect
import logging
from dataclasses import dataclass, replace, field
from enum import Enum, auto
from inspect import isfunction, isclass, ismethod, Parameter, signature
from typing import Any, Dict, Optional, Tuple, Union, Callable, Type, Sequence

from .constants import PARAMETER_WAS_NOT_EXPANDED, SINGLE_RUN
from .default.assign_results import assign
from .default.rw_classes import RunConfigurations, RwInstUpdater
from .launch_operations.errors import AssignmentError
from .utils.common import to_tuple, extract_attrpath
from .utils.options_utils import OptionsChecker
from .initialization_core import InitCore, is_it_init_arg_type, is_it_arg_type
from .utils.common import find_rw_inst
from .utils.formatters import LoggerBuilder, error_formatter

log = LoggerBuilder().build()

Cond = Callable[[Optional[Any]], bool]
ArgTuple = Tuple[Any, ...]
KwargsDict = Dict[str, Any]
HideLogInf = Tuple[bool, bool]
ParamsMap = Dict[str, Parameter]


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


def raise_with_stack(
        stack: Optional[str],
        exc_cls: Type[BaseException],
        message: str) -> None:
    stack_str = stack or "<unknown>"
    raise exc_cls(f"Operation: {stack_str}. {message}")


class OpBuilder:
    """Executes a captured call chain using InitCore to build args/kwargs at each CALL step."""
    @staticmethod
    def build_and_call(
        operation: "Operation",
        input_data: ArgTuple,
        rw_inst: Dict[str, Any],
        stack_divider: str = " -> ",
        br_stack: Optional[str] = None,
        op_stack_name: Optional[str] = None,
        hide_log_inf: HideLogInf = (False, False),
        check_type_strategy_all: bool = True,
    ) -> Tuple[Optional[Any], Optional[ArgTuple]]:

        OpBuilder._is_it_operation_check(op_stack_name, operation)
        obj = operation._obj

        hide_init_inf, hide_all_inf = hide_log_inf

        op_logger = OperationCallLogger(
            chain=obj._call_chain,
            br_stack=br_stack,
            op_stack_name=op_stack_name,
            divider=stack_divider,
            hide_init_inf=hide_init_inf,
            hide_all_inf=hide_all_inf)

        current = obj._call_chain.target
        rem_cursor: ArgTuple = input_data or ()

        for step in obj._call_chain.steps:
            if step.kind is StepKind.ATTR:
                if isinstance(step.name, str) and step.name.startswith("_"):
                    continue
                try:
                    current = getattr(current, step.name)
                except Exception as exc:
                    error_formatter(
                        exc, f"Operation: {op_stack_name}. "
                             f"Failed to access attr '{step.name}'.")
                    raise

            elif step.kind is StepKind.GETITEM:
                if (isinstance(step.key, tuple) and step.key and
                        OpBuilder._looks_like_operations_tuple(step.key)):
                    raise_with_stack(
                        op_stack_name,
                        TypeError,
                        "Looks like branch/operations tuple was used in item access. "
                        "CallObject[...] expects an index/slice/key.")
                try:
                    current = current[step.key]
                except Exception as exc:
                    error_formatter(
                        exc, f"Operation: {op_stack_name}. "
                             f"Failed to index with key '{step.key}'.")
                    raise exc

            elif step.kind is StepKind.CALL:
                params = OpBuilder._get_params_for_callable(current)
                call_args = OpBuilder._expand_special_args(
                    step.args, rw_inst)
                call_kwargs = OpBuilder._expand_special_kwargs(
                    step.kwargs, rw_inst)

                args, kwargs, rem = InitCore.get_args_kwargs(
                    op_stack_name,
                    params,
                    call_args,
                    call_kwargs,
                    rem_cursor,
                    check_type_strategy_all)

                op_logger.emit_current(args, kwargs)

                current = OpBuilder._invoke_callable(
                    op_stack_name, current, args, kwargs)
                rem_cursor = rem or ()

            else:
                raise_with_stack(
                    op_stack_name,
                    RuntimeError,
                    f"Unknown chain step kind: {step.kind}")

        return current, (rem_cursor or None)

    @staticmethod
    def _is_it_operation_check(stack: Optional[str], operation: Any):
        from .operation import Operation
        if not isinstance(operation, Operation):
            raise_with_stack(
                stack, TypeError,
                f"Operation must be class "
                f"Operation. Passed: {operation!r}")

    @staticmethod
    def _looks_like_operations_tuple(items: Tuple[Any, ...]) -> bool:
        try:
            from .operation import Operation
            from .branch import Branch
        except Exception:
            Operation = None  # type: ignore[assignment]
            Branch = None     # type: ignore[assignment]
        return all(
            (Operation is not None and isinstance(x, Operation)) or
            (Branch is not None and isinstance(x, Branch)) or
            isinstance(x, CallObject)
            for x in items)

    @staticmethod
    def _get_params_for_callable(callable_obj: Any) -> ParamsMap:
        if isclass(callable_obj):
            return (
                OpBuilder._get_params_wo_self(callable_obj.__init__)
                if "__init__" in vars(callable_obj)
                else OpBuilder._get_params_wo_self(
                    callable_obj, remove_first=False)
            )
        if ismethod(callable_obj):
            return OpBuilder._get_params_wo_self(callable_obj.__func__)
        return OpBuilder._get_params_wo_self(callable_obj, remove_first=False)

    @staticmethod
    def _get_params_wo_self(
            func: Callable, remove_first: bool = True) -> ParamsMap:
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
    def _invoke_callable(
            stack: Optional[str],
            target: Any,
            args: ArgTuple,
            kwargs: KwargsDict) -> Any:
        if isclass(target):
            try:
                instance = target(*args, **kwargs)
            except Exception as exc:
                error_formatter(
                    exc,
                    f"Operation: {stack}. Error "
                    f"while initializing class {target}")
                raise
            return instance
        try:
            return target(*args, **kwargs)
        except Exception as exc:
            error_formatter(exc, f"Operation: {stack}. "
                                 f"An error occurred while calling entity.")
            raise

    @staticmethod
    def _expand_special_kwargs(
            kwargs: Dict[str, Any], rw_inst: Dict[str, Any]) -> Dict[str, Any]:
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
        for param_name, param in list(kwargs.items()):
            if is_it_init_arg_type(param) and getattr(param, "param_link", None):
                parts = param.param_link.split(".")
                result = find_rw_inst(parts[0], rw_inst)
                if result:
                    if len(parts) == 1:
                        param.par_value = result
                    else:
                        for field in parts[1:]:
                            result = getattr(result, field)
                        param.par_value = result
                else:
                    param.par_value = PARAMETER_WAS_NOT_EXPANDED
                kwargs[param_name] = param
        return kwargs

    @staticmethod
    def _expand_special_args(
            args: Tuple[Any, ...], rw_inst: Dict[str, Any]) -> Tuple[Any, ...]:
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
            if is_it_init_arg_type(arg) and getattr(arg, "param_link", None):
                parts = arg.param_link.split(".")
                result = find_rw_inst(parts[0], rw_inst)
                if result:
                    if len(parts) == 1:
                        arg.par_value = result
                    else:
                        for field in parts[1:]:
                            result = getattr(result, field)
                        arg.par_value = result
                else:
                    arg.par_value = PARAMETER_WAS_NOT_EXPANDED
                new_args.append(arg)
            else:
                new_args.append(arg)
        return tuple(new_args)


class StepKind(Enum):
    ATTR = auto()     # .name
    CALL = auto()     # (...)
    GETITEM = auto()  # [key]


@dataclass(frozen=True)
class ChainStep:
    kind: StepKind
    name: Optional[str] = None                        # for ATTR
    args: ArgTuple = field(default_factory=tuple)     # for CALL
    kwargs: KwargsDict = field(default_factory=dict)  # for CALL
    key: Any = None                                   # for GETITEM


@dataclass(frozen=True)
class CallChain:
    target: Any
    steps: Tuple[ChainStep, ...] = field(default_factory=tuple)

    user_defined_op_name: Optional[str] = None
    base_is_instance: bool = False
    resolved_instance_cls_name: Optional[str] = None
    unresolved_external_path: Optional[str] = None

    def add_step(self, step: ChainStep) -> "CallChain":
        return replace(self, steps=self.steps + (step,))

    def with_user_op_name(self, name: str) -> "CallChain":
        return replace(self, user_defined_op_name=name)

    def with_resolved_instance(self, instance: Any) -> "CallChain":
        return replace(
            self,
            target=instance,
            base_is_instance=True,
            resolved_instance_cls_name=instance.__class__.__name__,
            unresolved_external_path=None,
        )

    def get_op_name(self) -> str:
        return self.user_defined_op_name or self._canonical_op_name()

    def _canonical_op_name(self) -> str:
        tgt = self.target

        def _base() -> str:
            if self.unresolved_external_path is not None:
                return (f'Instance from string: '
                        f'"{self.unresolved_external_path}"')

            if self.base_is_instance:
                return f"{(self.resolved_instance_cls_name or tgt.__class__.__name__)}(instance)"

            from inspect import isfunction, isclass
            if isfunction(tgt):
                return tgt.__name__
            if isclass(tgt):
                return tgt.__name__
            return tgt.__class__.__name__

        def _repr_key(k: Any) -> str:
            if isinstance(k, str):
                return f'"{k}"'
            if isinstance(k, slice):
                a = "" if k.start is None else k.start
                b = "" if k.stop is None else k.stop
                c = "" if k.step is None else k.step
                return f"{a}:{b}" + (f":{c}" if k.step is not None else "")
            return repr(k)

        name = _base()
        for st in self.steps:
            if st.kind is StepKind.ATTR:
                name = f"{name}.{st.name}"
            elif st.kind is StepKind.CALL:
                name = f"{name}()"
            elif st.kind is StepKind.GETITEM:
                name = f"{name}[{_repr_key(st.key)}]"
        return name or "UNKNOWN_ENTITY"


@dataclass
class CallChainPreviewer:
    """Builds a preview string for logs: ``{br_stack}{divider}{preview}``.

    Preview rules:
      * If ``chain.user_defined_op_name`` is set, use it and show the current CALL signature next to it.
      * Otherwise, iterate the chain and render:
          - ``ATTR`` → ``.name``
          - ``GETITEM`` → ``[key]`` (strings quoted, slices as ``a:b[:c]``)
          - ``CALL``:
              + past calls → ``()`` | ``(*args)`` | ``(**kwargs)`` | ``(*args, **kwargs)`` based on captured args in ``ChainStep``
              + current call → typed signature from ``current_args``/``current_kwargs``
              + future calls → ``(?)``
      * Base rendering:
          - class → ``ClassName``
          - function → ``func_name``
          - string → the path itself
          - ready instance → ``ClassName(instance)``

    Args:
        br_stack: Branch stack prefix (e.g., ``"br1 -> br2"``).
                 If provided, it will prefix the preview with ``{br_stack}{
                 divider}``.
        chain: The call chain to render.
        divider: Separator between ``br_stack`` and the preview.
    """

    br_stack: Optional[str]
    chain: CallChain
    divider: str = " -> "

    def format_line(
        self,
        *,
        cur_idx: int,
        current_args: ArgTuple,
        current_kwargs: KwargsDict,
    ) -> str:
        """Format a single preview line for the current CALL.

        ``cur_idx`` is the 0-based ordinal index among ``CALL`` steps only (ATTR/GETITEM are ignored).

        Args:
            cur_idx: Ordinal index of the current ``CALL`` within the chain (0-based).
            current_args: Resolved positional args for the current ``CALL``.
            current_kwargs: Resolved keyword args for the current ``CALL``.

        Returns:
            A preview string without the ``Operation: `` prefix.
        """
        body = self._format_body(
            cur_idx=cur_idx,
            current_args=current_args,
            current_kwargs=current_kwargs)
        if self.br_stack:
            return f"{self.br_stack}{self.divider}{body}"
        return f"{body}"

    def _format_body(
        self,
        *,
        cur_idx: int,
        current_args: ArgTuple,
        current_kwargs: KwargsDict,
    ) -> str:
        user_name = getattr(self.chain, "user_defined_op_name", None)
        if user_name:
            sig = self._format_current_signature(current_args, current_kwargs)
            return f"{user_name} {sig}"

        total_calls = sum(1 for s in self.chain.steps if s.kind is StepKind.CALL)
        if cur_idx < 0 or cur_idx >= total_calls:
            raise IndexError(f"cur_idx={cur_idx} out of "
                             f"range for {total_calls} CALL step(s)")

        parts: list[str] = [self._base_name(self.chain.target)]
        call_no = -1

        for st in self.chain.steps:
            if st.kind is StepKind.ATTR:
                parts.append(f".{st.name}")
            elif st.kind is StepKind.GETITEM:
                parts.append(self._format_key(st.key))
            elif st.kind is StepKind.CALL:
                call_no += 1
                if call_no < cur_idx:
                    parts.append(self._format_past_call(st))
                elif call_no == cur_idx:
                    parts.append(self._format_current_signature(
                        current_args, current_kwargs))
                else:
                    parts.append("(?)")
        return "".join(parts)

    @staticmethod
    def _base_name(target: Any) -> str:
        if isclass(target):
            return target.__name__
        if isfunction(target):
            return target.__name__
        if isinstance(target, str):
            return target
        return f"{target.__class__.__name__}(instance)"

    @staticmethod
    def _format_key(key: Any) -> str:
        if isinstance(key, str):
            return f'["{key}"]'
        if isinstance(key, slice):
            a = "" if key.start is None else key.start
            b = "" if key.stop is None else key.stop
            c = "" if key.step is None else key.step
            base = f"[{a}:{b}]"
            return base[:-1] + (f":{c}]" if key.step is not None else "]")
        return f"[{repr(key)}]"

    @staticmethod
    def _render_type_token(tok: Any) -> str:
        """Return just the class/type name (e.g., 'int', 'MyClass')."""
        t = tok if isinstance(tok, type) else type(tok)
        return getattr(t, "__name__", str(t))

    @classmethod
    def _compress_types(cls, tokens: Sequence[str]) -> str:
        """
          ['<class \'int\'>', '<class \'int\'>', '<class \'list\'>', '<class \'list\'>', '<class \'list\'>']
          -> "<class 'int'>, ...x2, <class 'list'>, ...x3"
        """
        if not tokens:
            return ""
        out: list[str] = []
        prev = tokens[0]
        cnt = 1
        for t in tokens[1:]:
            if t == prev:
                cnt += 1
            else:
                out.append(f"{prev}, ...x{cnt}" if cnt > 1 else prev)
                prev, cnt = t, 1
        out.append(f"{prev}, ...x{cnt}" if cnt > 1 else prev)
        return ", ".join(out)

    def _format_current_signature(self, args: ArgTuple,
                                  kwargs: KwargsDict) -> str:
        has_a = bool(args)
        has_k = bool(kwargs)

        a_part = ""
        if has_a:
            types = [self._render_type_token(a) for a in args]
            comp = self._compress_types(types)
            needs_trailing = (len(args) == 1) and ("...x" not in comp)
            a_part = f"*({comp}{',' if needs_trailing else ''})"

        k_part = ""
        if has_k:
            items = ", ".join(f"{k}: {self._render_type_token(v)}" for k, v in kwargs.items())
            k_part = f"**{{{items}}}"

        if has_a and has_k:
            return f"({a_part}, {k_part})"
        if has_a:
            return f"({a_part})"
        if has_k:
            return f"({k_part})"
        return "(w/o args)"

    @staticmethod
    def _format_past_call(step: ChainStep) -> str:
        has_a = bool(getattr(step, "args", ()))
        has_k = bool(getattr(step, "kwargs", {}))
        if has_a and has_k:
            return "(*args, **kwargs)"
        if has_a:
            return "(*args)"
        if has_k:
            return "(**kwargs)"
        return "()"


@dataclass
class OperationCallLogger:
    """Emits log lines for each CALL step in a CallChain.

    Behavior:
      - If hide_all_inf=True: do nothing.
      - If hide_init_inf=True: log 'Operation: {op_stack_name}' on every CALL.
      - Else: build a preview via CallChainPreviewer and log
              'Operation: {br_stack}{divider}{preview}' for the current CALL.

    cur_idx is managed internally and grows by 1 after each emit.
    """
    chain: CallChain
    br_stack: Optional[str]
    op_stack_name: Optional[str]
    divider: str = " -> "
    hide_init_inf: bool = False
    hide_all_inf: bool = False
    logger: Optional[logging.Logger] = None

    _cur_idx: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        if self.logger is None:
            self.logger = LoggerBuilder().build()

    def emit_current(self, args: ArgTuple, kwargs: KwargsDict) -> None:
        if self.hide_all_inf:
            return

        if self.hide_init_inf:
            header = self.op_stack_name or ""
            self.logger.info(f"Operation: {header}")
        else:
            previewer = CallChainPreviewer(
                br_stack=self.br_stack,
                chain=self.chain,
                divider=self.divider,
            )
            line = previewer.format_line(
                cur_idx=self._cur_idx,
                current_args=args,
                current_kwargs=kwargs,
            )
            self.logger.info(f"Operation: {line}")

        self._cur_idx += 1


class CallObject:
    """Captures function/class/instance call chains without executing them.

    Examples captured:
        func(1, m[int], "3")
        ManyRunMethods(1, m[int]).method3(5).property1["key"].other(7, x=8)
    """
    def __init__(self, cls_func_inst: Union[Callable, Type, Any]) -> None:
        proxy_path = extract_attrpath(
            cls_func_inst)
        if proxy_path is not None:
            cls_func_inst = proxy_path

        base_is_instance = (
            not isclass(cls_func_inst) and
            not isfunction(cls_func_inst) and
            not isinstance(cls_func_inst, str)
        )
        unresolved_external_path = cls_func_inst if isinstance(
            cls_func_inst, str) else None
        resolved_instance_cls_name = None
        if base_is_instance:
            resolved_instance_cls_name = cls_func_inst.__class__.__name__

        self._chain: CallChain = CallChain(
            target=cls_func_inst,
            steps=(),
            user_defined_op_name=None,
            base_is_instance=base_is_instance,
            resolved_instance_cls_name=resolved_instance_cls_name,
            unresolved_external_path=unresolved_external_path,
        )
        self._pending_misuse_msg: Optional[str] = None

    def __getattr__(self, name: str) -> "CallObject":
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        self._chain = self._chain.add_step(ChainStep(kind=StepKind.ATTR, name=name))
        return self

    def __call__(self, *args, **kwargs) -> "CallObject":
        self._chain = self._chain.add_step(ChainStep(kind=StepKind.CALL, args=args, kwargs=kwargs))
        return self

    def __getitem__(self, key: Any) -> "CallObject":
        self._chain = self._chain.add_step(ChainStep(kind=StepKind.GETITEM, key=key))
        return self

    def _get_instance_from_string(self, op_stack_name: str, rw_inst: Dict[str, Any]) -> None:
        """If base target is a string path into rw_inst, resolve it lazily."""
        if isinstance(self._chain.target, str):
            resolved = self._get_instance_from_str(op_stack_name, self._chain.target, rw_inst)
            self._chain = self._chain.with_resolved_instance(resolved)

    @staticmethod
    def _get_instance_from_str(stack: str, string: str, rw_inst: Dict[str, Any]) -> Any:
        """Parse string and find instance of executable entity of operation.

        For "t.bb_class.field", it will navigate rw_inst['t'].bb_class.field
        """
        parts = string.split(".")
        result = find_rw_inst(parts[0], rw_inst)
        if not result:
            existing_aliases = f"{list(rw_inst)}" if rw_inst else f"{rw_inst}"
            raise_with_stack(
                stack, TypeError,
                f'No such alias "{parts[0]}" in rw_inst. '
                f'Existing_aliases: {existing_aliases}.',
            )
        class_name = result.__class__.__name__
        for field in parts[1:]:
            try:
                result = getattr(result, field)
            except AttributeError:
                raise_with_stack(
                    stack, AttributeError,
                    f'The RW class "{class_name}" '
                    f'does not have attribute "{field}".',
                )
        return result

    def _try_to_find_slot_for_arg(self) -> bool:
        """Detect if any MandatoryArgTypeContainer present in CALL-steps."""
        for step in self._chain.steps:
            if step.kind is StepKind.CALL:
                for arg in list(step.args) + list(step.kwargs.values()):
                    init_arg_type = is_it_init_arg_type(arg)
                    arg_type = is_it_arg_type(arg)
                    if (init_arg_type == "optional") or (arg_type == "optional") or (
                        init_arg_type and getattr(arg, "param_link", None) is not None
                    ):
                        continue
                    if is_it_arg_type(arg):
                        return True
        return False

    def _set_user_defined_op_name(self, name: str) -> None:
        self._chain = self._chain.with_user_op_name(name)

    @property
    def _call_chain(self) -> "CallChain":
        return self._chain


@dataclass
class OperationOptions:
    op_name: Optional[str] = None
    rw_inst: Union[Dict[str, Any], Tuple[Dict[str, Any], ...]] = ()
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
        self._check_name(stack)

        canonical = self._obj._call_chain._canonical_op_name()
        cur = self._opts.op_name

        if not cur:
            self._opts = replace(self._opts, op_name=canonical)
            return

        if cur == canonical or cur.startswith("Instance from string"):
            self._opts = replace(self._opts, op_name=canonical)
            return

        self._obj._set_user_defined_op_name(cur)

    def _update_stack(self, run_conf: RunConfigurations) -> RunConfigurations:
        self._get_op_name(run_conf.last_op_stack)
        run_conf.set_operation_stack(self._opts.op_name)
        return run_conf

    def _update_rw_inst(self, run_conf: RunConfigurations) -> RunConfigurations:
        run_conf.update_last_rw_inst(self._opts.rw_inst)
        self._obj._get_instance_from_string(
            run_conf.operation_stack, run_conf.get_rw_inst())
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

    def assign(self, *args: Union[str, Tuple[str, ...]]) -> "BaseOperationMethods":
        self._opts = replace(self._opts, assign=args)
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
        def check_callable(op_stack: str, name: str,
                           condition: Optional[Callable]) -> None:
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
    def __init__(self, call_object: Union[CallObject, Any]) -> None:
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
            br_stack, stack_divider = None, None
        else:
            hide_log_inf, check_type_strategy_all, rw_inst = self._get_run_options(
                self._run_conf)
            op_stack = self._run_conf.operation_stack
            br_stack = self._run_conf.get_branch_stack()
            stack_divider = self._run_conf.stack_divider

        result, rem_args = OpBuilder.build_and_call(
            self,
            to_tuple(input_data),
            rw_inst,
            stack_divider,
            br_stack,
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
        fields_for_assign = Assigner._materialize_attrpaths_tuple(
            fields_for_assign)
        OptionsChecker.check_assign_option(
            stack, fields_for_assign, rw_inst)
        Assigner._validate_result(stack, result, fields_for_assign)
        kw = {key: rw_inst[key.split(".")[0]] for key in fields_for_assign}
        return assign(*to_tuple(result), **kw)

    @staticmethod
    def _materialize_attrpaths_tuple(
            items: Tuple[Any, ...]) -> Tuple[Any, ...]:
        """
        For each element in `items`, if it's an AttrProxy (per extract_attrpath),
        replace it with its string path; otherwise keep the element as-is.
        """

        def _one(x: Any) -> Any:
            path = extract_attrpath(x)
            return x if path is None else path

        return tuple(_one(x) for x in items)

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
