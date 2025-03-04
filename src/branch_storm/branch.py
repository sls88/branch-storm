from typing import Any, Dict, Optional, Tuple, Union

from .constants import STOP_CONSTANT
from .launch_operations.data_parsing import ResultParser
from .launch_operations.errors import EmptyDataError, IncorrectParameterError, EmptyBranchError, \
    RemainingArgsFoundError
from .utils.common import to_tuple
from .launch_operations.rw_inst_updater import RwInstUpdater
from .operation import Operation, CallObject, Assigner, OptionsChecker
from .utils.common import renew_def_rw_inst
from .utils.formatters import LoggerBuilder


log = LoggerBuilder().build()


BranchType = Union[Operation, "Branch", CallObject, Tuple[
    Union[Operation, "Branch", CallObject], ...]]


class Branch:
    def __init__(self, br_name: str = None) -> None:
        self._operations: Optional[Tuple] = None
        self._current_operation: Optional[Union[Branch, Operation]] = None

        self._br_name: Optional[str] = None
        self.set_br_name(br_name)
        self._def_args: Optional[Tuple] = None
        self._assign: Optional[Tuple[str]] = None
        self._all_operations_must_be_executed: Optional[bool] = None
        self._hide_init_inf_from_logs: Optional[bool] = None
        self._check_type_strategy_all: Optional[bool] = None
        self._raise_err_if_empty_data: bool = False
        self._distribute_input_data: bool = False

        self._rw_inst: Optional[Dict[str, Any]] = None
        self._rw_inst_from_option: Optional[Dict[str, Any]] = None

        self._branch_stack = "INITIAL RUN"
        self._operation_stack = "INITIAL RUN"
        self._last_op_stack = "INITIAL RUN"
        self._is_it_operation = False
        self._delayed_return = None

    def def_args(self, *def_args: Tuple[Any, ...]) -> "Branch":
        self._def_args = def_args
        return self

    def rw_inst(self, rw_inst: Dict[str, Any]) -> "Branch":
        self._rw_inst_from_option = rw_inst
        return self

    def assign(self, *args: Tuple[str, ...]) -> "Branch":
        self._assign = args
        return self

    def all_operations_must_be_executed(self, value: bool) -> "Branch":
        self._all_operations_must_be_executed = value
        return self

    def hide_init_inf_from_logs(self, value: bool) -> "Branch":
        self._hide_init_inf_from_logs = value
        return self

    def check_type_strategy_all(self, value: bool) -> "Branch":
        self._check_type_strategy_all = value
        return self

    @property
    def distribute_input_data(self) -> "Branch":
        self._distribute_input_data = True
        return self

    @property
    def raise_err_if_empty_data(self) -> "Branch":
        self._raise_err_if_empty_data = True
        return self

    def get_br_name(self) -> str:
        return self._br_name

    def set_br_name(self, br_name: Optional[str]) -> None:
        self._br_name = "BRANCH NAME NOT DEFINED" \
            if br_name is None else br_name

    def _renew_def_rw_inst(self):
        self._rw_inst = renew_def_rw_inst(self._operation_stack, self._rw_inst)

    def _get_current_operation(self) -> None:
        if self._operations:
            self._current_operation = self._operations[0]
            if not any([
                isinstance(self._current_operation, Branch),
                isinstance(self._current_operation, Operation),
                isinstance(self._current_operation, CallObject)]):
                raise TypeError(
                    f"Last successful operation: {self._last_op_stack}.\n"
                    f"Processing object can be only Branch, Operation, CallObject.")

            if isinstance(self._current_operation, CallObject):
                self._current_operation = Operation(self._current_operation)

            self._is_it_operation = True if isinstance(
                self._current_operation, Operation) else False

            self._operations = None if len(
                self._operations) == 1 else self._operations[1:]
        else:
            raise EmptyBranchError(
                f"Operation: {self._operation_stack}.\n"
                f"Running a branch without operations is impossible.")

    def _get_curr_op_params(self):
        self._def_args = self._current_operation._def_args
        self._raise_err_if_empty_data = self._current_operation._raise_err_if_empty_data

    def __getitem__(self, operations: BranchType) -> "Branch":
        operations = to_tuple(operations)
        self._operations = operations
        return self

    def __call__(self, *args: BranchType, **kwargs) -> "Branch":
        if self._operations is None:
            args = to_tuple(args)
            self._operations = args
        return self

    def _set_branch_stack(self, stack: str = None):
        if self._br_name is None:
            self._br_name = "BRANCH NAME NOT DEFINED"
        if stack is not None:
            self._branch_stack = f"{stack} -> {self._br_name}"

    def _set_last_op_stack(self, stack: str) -> None:
        self._last_op_stack = stack

    def _set_rw_inst(self, rw_inst: Dict[str, Any]) -> None:
        self._rw_inst = rw_inst

    def _update_rw_inst(self, rw_inst: Dict[str, Any]) -> None:
        self._rw_inst = RwInstUpdater.get_updated(
            self._operation_stack, self._rw_inst, rw_inst)

    def _get_initial_run_params(self, input_data: Optional[Any] = None) -> Optional[Any]:
        if input_data is None and self._branch_stack == \
                "INITIAL RUN" and self._def_args is None:
            input_data = ()
        if self._branch_stack == "INITIAL RUN":
            self._branch_stack = self._br_name
            if self._all_operations_must_be_executed is None:
                self._all_operations_must_be_executed = False
            if self._hide_init_inf_from_logs is None:
                self._hide_init_inf_from_logs = False
            if self._check_type_strategy_all is None:
                self._check_type_strategy_all = True
        return input_data

    def _end_branch_check(
            self, stop_operations: Optional[bool] = None) -> None:
        if stop_operations and self._all_operations_must_be_executed:
            raise IncorrectParameterError(
                f'\nOperation: {self._operation_stack}.\n'
                f'The previous operation returned the constant\n'
                f'"stop_all_further_operations_with_success_result"\n'
                f'meaning a forced stop of all further operations,\n'
                f'but it was found that the current branch has the\n'
                f'option all_operations_must_be_executed=True applied.\n'
                f'Combining these factors is impossible.')
        elif self._all_operations_must_be_executed:
            raise EmptyDataError(
                f"Operation: {self._operation_stack}.\n"
                f"The data was not received when all operations were\n"
                f"scheduled to be performed.")
        elif self._raise_err_if_empty_data and not stop_operations:
            raise EmptyDataError(
                f"Operation: {self._operation_stack}.\n"
                f"No data was received. An exception was raised,\n"
                f"according to the Operation/Branch.raise_err_if_empty_data flag set.")

    def run(self, input_data: Optional[Any] = None) -> Optional[Any]:
        self._get_current_operation()
        self._get_curr_op_params()
        input_data = self._get_initial_run_params(input_data)
        OptionsChecker.check_name(self._br_name, self._last_op_stack)
        self._set_branch_stack()
        if self._is_it_operation:
            self._current_operation._set_last_op_stack(self._last_op_stack)
            self._current_operation._set_branch_stack(self._branch_stack)
            self._current_operation._check_name()
            self._operation_stack = self._current_operation._update_stack()
            OptionsChecker.check_burn_rem_args_br(
                self._operation_stack,
                self._current_operation._burn_rem_args,
                self._current_operation._stop_distribution,
                self._delayed_return)
        else:
            self._current_operation._set_last_op_stack(self._last_op_stack)
            self._operation_stack = f"{self._branch_stack}(branch)"

        self._update_rw_inst(self._rw_inst_from_option)

        if input_data is None and self._def_args is not None:
            input_data = self._def_args
        elif input_data is None and self._def_args is None:
            self._end_branch_check()
            return None
        sd, self._rw_inst = ResultParser.sort_data(to_tuple(input_data), self._rw_inst)
        if sd.stop_all_operations:
            self._end_branch_check(
                sd.stop_all_operations)
            log.info(
                f'Operation: {self._operation_stack}.\n'
                f'The operation returned the constant '
                f'"stop_all_further_operations_with_success_result"\n'
                f'meaning forced stop of all further operations. '
                f'The branch will return this constant as a result.')
            return STOP_CONSTANT
        input_data = sd.data

        if self._is_it_operation:
            self._current_operation._update_rw_inst(self._rw_inst)
            self._operation_stack = self._current_operation._get_op_stack()
            if self._current_operation._hide_init_inf_from_logs is None:
                self._current_operation._hide_init_inf_from_logs = self._hide_init_inf_from_logs
            if self._current_operation._check_type_strategy_all is None:
                self._current_operation._check_type_strategy_all = self._check_type_strategy_all

            result, rem_args = self._current_operation.run(input_data)

            if self._operations is None and self._delayed_return is not None:
                self._current_operation._stop_distribution = True

            if self._delayed_return is not None and not \
                    self._current_operation._stop_distribution or self._distribute_input_data:
                self._distribute_input_data = False
                self._current_operation._distribute_input_data = True

            if self._current_operation._distribute_input_data and not self._delayed_return:
                self._delayed_return = to_tuple(result)
                result = () if rem_args is None else rem_args
            elif self._delayed_return:
                self._delayed_return = (*self._delayed_return, *to_tuple(result))
                result = () if rem_args is None else rem_args
            if self._current_operation._stop_distribution:
                result = self._delayed_return[0] if len(
                    self._delayed_return) == 1 else self._delayed_return
                self._delayed_return = None

            if rem_args is not None and not any([
                self._delayed_return is not None,
                self._distribute_input_data]):
                rem_args_hidden = [type(arg) for arg in rem_args]
                raise RemainingArgsFoundError(
                    f"Operation: {self._operation_stack}.\n"
                    f"After executing the operation, data was detected that was not involved\n"
                    f"in the initialization/call. Len {len(rem_args)}; Their types: {rem_args_hidden}\n"
                    f"If this is planned, use the burn_rem_args option or use the distribution operation\n"
                    f"(distributed_input_data ... stop_distribution options).\n"
                    f"After stopping the distribution, the remaining arguments are also not allowed.")

            self._set_last_op_stack(self._current_operation._get_op_stack())

            if self._operations is None:
                return result
            result = self.run(result)
            return result

        if self._current_operation._all_operations_must_be_executed is None:
            self._current_operation._all_operations_must_be_executed = \
                self._all_operations_must_be_executed
        if self._current_operation._hide_init_inf_from_logs is None:
            self._current_operation._hide_init_inf_from_logs = \
                self._hide_init_inf_from_logs
        if self._current_operation._check_type_strategy_all is None:
            self._current_operation._check_type_strategy_all = \
                self._check_type_strategy_all
        if self._distribute_input_data:
            self._distribute_input_data = False
            self._current_operation._distribute_input_data = True

        self._current_operation._set_branch_stack(self._branch_stack)
        self._current_operation._set_rw_inst(self._rw_inst)
        self._current_operation._renew_def_rw_inst()

        result = self._current_operation.run(input_data)
        self._set_last_op_stack(self._current_operation._last_op_stack)

        if self._operations is None:
            if self._assign is not None:
                return Assigner.do_assign(
                    self._operation_stack, self._assign,
                    self._rw_inst, result)
            return result

        result = self.run(result)
        if self._assign is not None:
            return Assigner.do_assign(
                self._operation_stack, self._assign,
                self._rw_inst, result)
        return result
