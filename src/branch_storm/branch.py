from abc import ABC, abstractmethod
from dataclasses import replace
from typing import Any, Dict, Optional, Tuple, Union, Callable, Type

from .constants import STOP_CONSTANT, SKIP_OPERATION_CONSTANT, INITIAL
from .default.rw_classes import RunConfigurations, BranchOptions
from .launch_operations.capture_manager import begin_capture, end_capture
from .launch_operations.data_parsing import ResultParser
from .launch_operations.errors import RemainingArgsFoundError, ConditionNotMetError
from .utils.common import to_tuple
from .operation import Operation, CallObject, OptionsChecker, do_assign_result
from .utils.formatters import LoggerBuilder


log = LoggerBuilder().build()


BranchType = Union[Operation, "Branch", CallObject, Tuple[
    Union[Operation, "Branch", CallObject], ...], Any]

CurrOpType = Union["Branch", Operation]


def get_run_config(
        rw_inst_opt: Tuple[Dict[str, Any], ...] = None,
        input_data: Optional[Any] = None
) -> Tuple[RunConfigurations, Optional[Any]]:
    run_conf = None
    if isinstance(input_data, RunConfigurations):
        return input_data, None
    if isinstance(input_data, Tuple):
        input_data, run_conf = ResultParser.separate_run_conf(input_data)
    if not run_conf:
        run_conf = ResultParser.find_run_conf_from_rw_inst(rw_inst_opt)
    if run_conf:
        return run_conf, input_data
    return RunConfigurations(), input_data


def data_separation(
        input_data: Optional[Any], run_conf: RunConfigurations
        ) -> Tuple[Tuple, RunConfigurations, Optional[str], Optional[str]]:
    sd, rw_inst = ResultParser.sort_data(
        input_data, run_conf.get_rw_inst())
    run_conf.update_last_rw_inst((rw_inst,))
    stop_constant, second_stop_constant = None, None
    if sd.stop_constant:
        stop_constant = STOP_CONSTANT
    if sd.skip_operation_constant:
        second_stop_constant = SKIP_OPERATION_CONSTANT
    input_data = sd.data
    return input_data, run_conf, stop_constant, second_stop_constant


class Processor(ABC):
    @staticmethod
    @abstractmethod
    def run(operations: Tuple[Union[Operation, CallObject, "Branch"], ...],
            run_conf: RunConfigurations,
            input_data: Optional[Any] = None,
            rem_args: Optional[Tuple] = None
            ) -> Tuple[Optional[Any], RunConfigurations]:
        pass


class _BrShared:
    @staticmethod
    def _prepare_operation(
            curr_op: Any,
            run_conf: RunConfigurations
    ) -> Tuple[CurrOpType, RunConfigurations, bool, str]:
        _BrShared._curr_op_check(curr_op, run_conf.last_op_stack)
        curr_op = _BrShared._wrap_call_object(curr_op)
        is_operation = _BrShared._get_is_it_operation_flag(curr_op)

        if is_operation:
            run_conf = curr_op._update_stack(run_conf)
            _BrShared._check_options(curr_op, run_conf)
            run_conf = curr_op._update_rw_inst(run_conf)

        return curr_op, run_conf, is_operation, run_conf.operation_stack

    @staticmethod
    def _pull_options_and_flags(
            curr_op: CurrOpType,
            input_data: Optional[Any],
            op_stack: str
    ) -> Tuple[Callable, Callable, bool, bool, bool]:
        end_chain_cond, raise_err_cond, force_call = curr_op._pull_options()
        _BrShared._check_passed_conditions(
            op_stack, end_chain_cond, raise_err_cond)
        end_flag, raise_flag = _BrShared._get_end_conditions_flags(
            end_chain_cond, raise_err_cond, input_data
        )
        return end_chain_cond, raise_err_cond, force_call, end_flag, raise_flag

    @staticmethod
    def _execute_step(
            is_operation: bool,
            has_next: bool,
            curr_op: CurrOpType,
            run_conf: RunConfigurations,
            input_data: Optional[Any]
    ) -> Tuple[Optional[Any], Optional[Tuple], RunConfigurations]:
        if is_operation:
            curr_op._set_run_conf(run_conf)
            result, rem_args = curr_op.run(input_data)
        else:
            result, rem_args, run_conf = curr_op.rw_inst(
                {"run_conf": run_conf}).run(input_data)

        run_conf = ArgsDistributor.open_distribution(curr_op, run_conf)
        result, run_conf = ArgsDistributor.continue_distribution(
            run_conf, result, rem_args)
        result, run_conf = ArgsDistributor.stop_distribution(
            has_next, curr_op, run_conf, result)

        run_conf.last_op_stack = run_conf.operation_stack
        return result, rem_args, run_conf

    @staticmethod
    def _end_branch_check(
            operation_stack: str,
            input_data: Optional[Any],
            raise_err_flag: bool,
            raise_err_cond: Callable) -> None:
        if input_data is STOP_CONSTANT or input_data is \
                SKIP_OPERATION_CONSTANT:
            return

        if raise_err_flag:
            types = []
            if isinstance(input_data, Tuple
                          ) and len(input_data) > 1:
                for ty in input_data:
                    types.append(ty)
            else:
                types.append(input_data)
            data_hidden = tuple(list(map(
                lambda x: type(x), types)))
            raise ConditionNotMetError(
                f"Operation: {operation_stack}.\n"
                f"Received data: {data_hidden}"
                f"(their types show) "
                f"does not match the passed "
                f"condition: {raise_err_cond}\n"
                f"passed via option raise_err_if"
                f"(condition_func: Callable)")

    @staticmethod
    def _log_info_mess_cond(
            op_stack: str,
            end_chain_cond: Callable) -> None:
        log.info(
            f'Operation: {op_stack}.\n'
            f'The received data corresponds to '
            f'the condition:\n{end_chain_cond}\n'
            f'passed via the end_chain_if'
            f'(condition_func: Callable) option.\n'
            f'The branch will receive stop '
            f'constant and the chain of further\n'
            f'calls will be stopped without an error.')

    @staticmethod
    def _check_passed_conditions(
            op_stack: str, end_chain_cond: Callable,
            raise_err_cond: Callable) -> None:
        def check_condition(op_stack: str,
                            condition: Callable) -> None:
            """Applies a condition to input data after verifying it's callable.

            Args:
                condition: Callable to be applied to input_data

            Raises:
                TypeError: If condition is not callable
            """
            if condition is not None and not callable(condition):
                raise TypeError(
                    f"Operation: {op_stack}.\n"
                    f"Condition must be callable "
                    f"(e.g., function, lambda)")

        check_condition(op_stack, end_chain_cond)
        check_condition(op_stack, raise_err_cond)

    @staticmethod
    def _get_end_conditions_flags(
            end_chain_cond: Callable,
            raise_err_cond: Callable,
            input_data: Optional[Any] = None) -> Tuple[bool, bool]:
        if input_data is STOP_CONSTANT or input_data is \
                SKIP_OPERATION_CONSTANT:
            return False, False

        end_chain_flag = end_chain_cond(input_data) \
            if end_chain_cond is not None else False
        raise_err_flag = raise_err_cond(input_data) \
            if raise_err_cond is not None else False
        return end_chain_flag, raise_err_flag

    @staticmethod
    def _check_options(
            curr_op: CurrOpType,
            run_conf: RunConfigurations) -> None:
        OptionsChecker.check_burn_rem_args_br(
            run_conf.operation_stack,
            curr_op._opts.burn_rem_args,
            curr_op._opts.stop_distribution,
            run_conf.br_opt.delayed_return)

    @staticmethod
    def _get_is_it_operation_flag(curr_op: CurrOpType) -> bool:
        return isinstance(curr_op, Operation)

    @staticmethod
    def _curr_op_check(curr_op: Any, last_op_stack: str) -> None:
        if not isinstance(curr_op, (Branch, Operation, CallObject)):
            raise TypeError(
                f"Last successful operation: "
                f"{last_op_stack}.\n"
                f"Processing object can be only "
                f"Branch, Operation, CallObject.")

    @staticmethod
    def _wrap_call_object(
            curr_op: Union["Branch", Operation, CallObject]) -> CurrOpType:
        if isinstance(curr_op, CallObject):
            return Operation(curr_op)
        return curr_op

    @staticmethod
    def _handle_entrypoint_result(result: Optional[Any],
            rem_args: Optional[Tuple],
            run_conf: RunConfigurations, ) -> Tuple[
        Optional[Any], Optional[Tuple], RunConfigurations]:
        br_opt = run_conf.br_opt

        if br_opt.entrypoint_processed:
            return result, rem_args, run_conf

        if (not br_opt.take_all_args) and (rem_args is not None):
            br_opt.entrypoint_rem_args = rem_args
            rem_args = None

            if ArgsDistributor.is_collecting(run_conf):
                result = ()

        br_opt.entrypoint_processed = True
        return result, rem_args, run_conf


class BrIterativeProcessor(_BrShared, Processor):
    @staticmethod
    def run(
        operations: Tuple[Union[Operation, CallObject, "Branch"], ...],
        run_conf: RunConfigurations,
        input_data: Optional[Any] = None,
        rem_args: Optional[Tuple] = None
    ) -> Tuple[Optional[Any], Optional[Any], RunConfigurations]:
        if not operations:
            return input_data, rem_args, run_conf

        n = len(operations)
        idx = 0
        while idx < n:
            curr_op = operations[idx]
            has_next = idx < n - 1

            curr_op, run_conf, is_operation, op_stack = (
                BrIterativeProcessor._prepare_operation(curr_op, run_conf))

            (end_chain_cond, raise_err_cond, force_call,
             end_chain_flag, raise_err_flag) = \
                BrIterativeProcessor._pull_options_and_flags(
                    curr_op, input_data, op_stack)

            if end_chain_flag:
                BrIterativeProcessor._log_info_mess_cond(
                    op_stack, end_chain_cond)
                input_data = SKIP_OPERATION_CONSTANT
                idx += 1
                continue

            BrIterativeProcessor._end_branch_check(
                op_stack, input_data, raise_err_flag, raise_err_cond)

            (input_data, run_conf, stop_constant,
             skip_operation_constant) = data_separation(
                input_data, run_conf)
            if (stop_constant or skip_operation_constant) and not force_call:
                return STOP_CONSTANT, None, run_conf

            result, rem_args, run_conf = BrIterativeProcessor._execute_step(
                is_operation, has_next, curr_op, run_conf, input_data)

            result, rem_args, run_conf = BrIterativeProcessor._handle_entrypoint_result(
                result, rem_args, run_conf)

            ArgsDistributor.rem_args_check(
                is_operation, op_stack, rem_args, run_conf)

            (result, run_conf, stop_constant,
             skip_operation_constant) = data_separation(
                result, run_conf)
            if stop_constant or skip_operation_constant:
                input_data = SKIP_OPERATION_CONSTANT
                idx += 1
                continue

            input_data = result
            idx += 1

        return input_data, rem_args, run_conf


class BrRecursiveProcessor(_BrShared, Processor):
    @staticmethod
    def run(
        operations: Tuple[Union[Operation, CallObject, "Branch"], ...],
        run_conf: RunConfigurations,
        input_data: Optional[Any] = None,
        rem_args: Optional[Tuple] = None
    ) -> Tuple[Optional[Any], Optional[Any], RunConfigurations]:
        if not operations:
            return input_data, rem_args, run_conf

        curr_op, remaining_ops = operations[0], operations[1:]

        (curr_op, run_conf, is_operation,
         op_stack) = BrRecursiveProcessor._prepare_operation(
            curr_op, run_conf)

        (end_chain_cond, raise_err_cond, force_call,
         end_chain_flag, raise_err_flag) = \
            BrRecursiveProcessor._pull_options_and_flags(
                curr_op, input_data, op_stack)

        if end_chain_flag:
            BrRecursiveProcessor._log_info_mess_cond(op_stack, end_chain_cond)
            return BrRecursiveProcessor.run(
                remaining_ops, run_conf, SKIP_OPERATION_CONSTANT, rem_args)

        BrRecursiveProcessor._end_branch_check(
            op_stack, input_data, raise_err_flag, raise_err_cond)

        (input_data, run_conf, stop_constant,
         skip_operation_constant) = data_separation(
            input_data, run_conf)
        if (stop_constant or skip_operation_constant) and not force_call:
            return STOP_CONSTANT, None, run_conf

        has_next = bool(remaining_ops)

        result, rem_args, run_conf = BrRecursiveProcessor._execute_step(
            is_operation, has_next, curr_op, run_conf, input_data)

        result, rem_args, run_conf = BrIterativeProcessor._handle_entrypoint_result(
            result, rem_args, run_conf)

        ArgsDistributor.rem_args_check(
            is_operation, op_stack, rem_args, run_conf)

        (result, run_conf, stop_constant,
         skip_operation_constant) = data_separation(
            result, run_conf)
        if stop_constant or skip_operation_constant:
            return BrRecursiveProcessor.run(
                remaining_ops, run_conf, SKIP_OPERATION_CONSTANT, rem_args)

        return BrRecursiveProcessor.run(
            remaining_ops, run_conf, result, rem_args)


class BaseBranchMethods:
    def __init__(self):
        self._is_it_first_branch: bool = False

    def _set_first_branch(self, branch_stack: str) -> None:
        if branch_stack == INITIAL:
            self._is_it_first_branch = True

    @staticmethod
    def _remove_stop_constant(result: Any) -> Any:
        if result is STOP_CONSTANT or result is SKIP_OPERATION_CONSTANT:
            return None
        return result


class Branch(BaseBranchMethods):
    def __init__(self, br_name: str = None,
                 processor: Optional[Type["Processor"]] = None) -> None:
        super().__init__()
        self._opts = BranchOptions(br_name=br_name, processor=processor)
        self._operations: Optional[Tuple] = None
        self._cap_session = begin_capture()
        self._cap_active: bool = True

    def end_chain_if(self, condition_func: Callable) -> "Branch":
        self._opts = replace(self._opts, end_chain_cond=condition_func)
        return self

    def raise_err_if(self, condition_func: Callable) -> "Branch":
        self._opts = replace(self._opts, raise_err_cond=condition_func)
        return self

    def rw_inst(self, rw_inst: Dict[str, Any]) -> "Branch":
        self._opts = replace(self._opts, rw_inst=self._opts.rw_inst + (rw_inst,))
        return self

    def assign(self, *args: Union[str, Tuple[str, ...]]) -> "Branch":
        self._opts = replace(self._opts, assign=args)
        return self

    def hide_log_inf(self, init_inf: bool = None, all_inf: bool = None) -> "Branch":
        self._opts = replace(self._opts, hide_log_inf=(init_inf, all_inf))
        return self

    def check_type_strategy_all(self, value: bool) -> "Branch":
        self._opts = replace(self._opts, check_type_strategy_all=value)
        return self

    @property
    def distribute_input_data(self) -> "Branch":
        self._opts = replace(self._opts, distribute_input_data=True)
        return self

    @property
    def stop_distribution(self) -> "Branch":
        self._opts = replace(self._opts, stop_distribution=True)
        return self

    @property
    def burn_rem_args(self) -> "Branch":
        self._opts = replace(self._opts, burn_rem_args=True)
        return self

    @property
    def take_all_args(self) -> "Branch":
        self._opts = replace(self._opts, take_all_args=True)
        return self

    @property
    def force_call(self) -> "Branch":
        self._opts = replace(self._opts, force_call=True)
        return self

    def _pull_options(self) -> Tuple[Callable, Callable, bool]:
        return (self._opts.end_chain_cond,
                self._opts.raise_err_cond,
                self._opts.force_call)

    def _seal_operations(self, operations: "BranchType") -> "Branch":
        """
        Set the operations exactly once and end the capture session.
        Both __getitem__ and __call__ route here.
        """
        self._operations = to_tuple(operations)
        if self._cap_active:
            end_capture(self._cap_session)
            self._cap_active = False
        return self

    def __getitem__(self, operations: "BranchType") -> "Branch":
        return self._seal_operations(operations)

    def __call__(self, *operations: "BranchType") -> "Branch":
        return self._seal_operations(operations)

    def run(self, input_data: Optional[Any] = None) -> Optional[Any]:
        run_conf, input_data = get_run_config(self._opts.rw_inst, input_data)
        self._set_first_branch(run_conf.get_branch_stack())

        run_conf.set_default_processor(BrIterativeProcessor)

        br_opt = self._opts.get_new_instance()
        run_conf.add_br_opt_to_stack(br_opt)

        result, rem_args, run_conf = run_conf.br_opt.processor.run(
            self._operations, run_conf, input_data)

        op_stack = run_conf.get_branch_stack()
        result = do_assign_result(
            op_stack, self._opts.assign,
            result, run_conf.get_rw_inst())

        if run_conf.br_opt.entrypoint_rem_args is not None:
            rem_args = run_conf.br_opt.entrypoint_rem_args

        if self._opts.burn_rem_args:
            rem_args = None

        ArgsDistributor.rem_args_br_check(
            op_stack, rem_args, self._is_it_first_branch)

        run_conf.pop_stack()
        if self._is_it_first_branch:
            return BaseBranchMethods._remove_stop_constant(result)
        return result, rem_args, run_conf


class ArgsDistributor:
    @staticmethod
    def is_distribution_active(run_conf: "RunConfigurations") -> bool:
        """
        Historical semantics:
        opened => delayed_return == ()
        active => delayed_return is not None
        stopped => delayed_return == None

        So "active" here really means "distribution is not stopped".
        """
        return run_conf.br_opt.delayed_return is not None

    @staticmethod
    def open_distribution(
        curr_op: Union["Operation", "Branch"],
        run_conf: "RunConfigurations",
    ) -> "RunConfigurations":
        br_opt = run_conf.br_opt

        if ArgsDistributor._is_stopped(run_conf) and curr_op._opts.distribute_input_data:
            br_opt.delayed_return = ()
            br_opt.distribute_input_data = True

        return run_conf

    @staticmethod
    def continue_distribution(
        run_conf: "RunConfigurations",
        result: Any,
        rem_args: Any,
    ) -> Tuple[Any, "RunConfigurations"]:
        if not ArgsDistributor.is_collecting(run_conf):
            return result, run_conf

        br_opt = run_conf.br_opt
        delayed = br_opt.delayed_return

        if not delayed and rem_args is None and isinstance(
                result, tuple) and len(result) > 1:
            return result, run_conf

        br_opt.delayed_return = ArgsDistributor._append_delayed(
            delayed, result)
        forwarded = () if rem_args is None else rem_args
        return forwarded, run_conf

    @staticmethod
    def stop_distribution(
        has_next: bool,
        curr_op: Union["Operation", "Branch"],
        run_conf: "RunConfigurations",
        result: Any,
    ) -> Tuple[Any, "RunConfigurations"]:
        br_opt = run_conf.br_opt

        should_stop = (
            curr_op._opts.stop_distribution
            or (not has_next and
                ArgsDistributor.is_distribution_active(run_conf)))
        if not should_stop:
            return result, run_conf

        delayed = br_opt.delayed_return

        if delayed is None:
            br_opt.delayed_return = None
            br_opt.distribute_input_data = False
            return result, run_conf

        result = delayed[0] if len(delayed) == 1 else delayed
        br_opt.delayed_return = None
        br_opt.distribute_input_data = False
        return result, run_conf

    @staticmethod
    def rem_args_br_check(
        stack: str,
        rem_args: Optional[Tuple],
        is_it_first_branch: bool,
    ) -> None:
        if is_it_first_branch and rem_args is not None:
            raise RemainingArgsFoundError(
                ArgsDistributor._build_remaining_args_error(
                    "Branch", stack, rem_args))

    @staticmethod
    def rem_args_check(
        is_operation: bool,
        stack: str,
        rem_args: Optional[Tuple],
        run_conf: "RunConfigurations",
    ) -> None:
        if rem_args is None:
            return
        if ArgsDistributor.is_distribution_active(run_conf):
            return

        kind = "Operation" if is_operation else "Branch"
        raise RemainingArgsFoundError(
            ArgsDistributor._build_remaining_args_error(kind, stack, rem_args))

    @staticmethod
    def _is_stopped(run_conf: "RunConfigurations") -> bool:
        return not ArgsDistributor.is_distribution_active(run_conf)

    @staticmethod
    def is_collecting(run_conf: "RunConfigurations") -> bool:
        return (ArgsDistributor.is_distribution_active(run_conf) and
                run_conf.br_opt.distribute_input_data)

    @staticmethod
    def _append_delayed(delayed_return: Tuple, value: Any) -> Tuple:
        return (*delayed_return, *to_tuple(value))

    @staticmethod
    def _build_remaining_args_error(kind: str, stack: str, rem_args: Tuple) -> str:
        rem_args_hidden = [type(arg) for arg in rem_args]
        kind_l = kind.lower()
        return (
            f"{kind}: {stack}.\n"
            f"After executing the {kind_l}, data was "
            f"detected that was not involved\n"
            f"in the initialization/call. "
            f"Len {len(rem_args)}; Their types: {rem_args_hidden}\n"
            f"If this is planned, use the burn_rem_args "
            f"option or use the distribution operation\n"
            f"(distributed_input_data ... stop_distribution options).\n"
            f"After stopping the distribution, the "
            f"remaining arguments are also not allowed.")
