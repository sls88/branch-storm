from typing import Any, Dict, Optional, Tuple, Union

from src.default.assign_results import assign
from src.launch_operations.data_parsing import ResultParser
from src.launch_operations.errors import EmptyDataError, IncorrectParameterError, EmptyBranchError
from src.launch_operations.launch_utils import to_tuple
from src.launch_operations.rw_inst_updater import RwInstUpdater
from src.operation import Operation
from src.utils.common import renew_def_rw_inst

BranchType = Union[Operation, "Branch", Tuple[Union[Operation, "Branch"], ...]]


class Branch:
    def __init__(self, br_name: str = None) -> None:
        self._operations: Optional[Tuple] = None
        self._current_operation: Optional[Union[Branch, Operation]] = None

        self._br_name: str = Branch._get_br_name(br_name)
        self._def_args: Optional[Tuple] = None
        self._assign: Optional[Tuple[str]] = None
        self._all_operations_must_be_executed: Optional[bool] = None
        self._hide_init_inf_from_logs: Optional[bool] = None
        self._raise_err_if_empty_data: bool = False
        self._distribute_input_data: bool = False

        self._rw_inst: Optional[Dict[str, Any]] = None
        self._rw_inst_from_option: Optional[Dict[str, Any]] = None

        self._branch_stack = "INITIAL RUN"
        self._operation_stack = "INITIAL RUN"
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

    def renew_def_rw_inst(self):
        self._rw_inst = renew_def_rw_inst(self._rw_inst)

    @property
    def distribute_input_data(self) -> "Branch":
        self._distribute_input_data = True
        return self

    @property
    def raise_err_if_empty_data(self) -> "Branch":
        self._raise_err_if_empty_data = True
        return self

    @staticmethod
    def _get_br_name(br_name: Optional[str]) -> str:
        return "BRANCH NAME NOT DEFINED" \
            if br_name is None else br_name

    def _get_current_operation(self) -> None:
        if self._operations:
            self._current_operation = self._operations[0]

            self._is_it_operation = True if isinstance(
                self._current_operation, Operation) else False

            self._operations = None if len(
                self._operations) == 1 else self._operations[1:]
        else:
            raise EmptyBranchError(
                f"Operation: {self._operation_stack} (last successful operation).\n"
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
        return input_data

    def _end_branch_check(
            self, stop_operations: Optional[bool] = None) -> None:
        if stop_operations and self._all_operations_must_be_executed:
            raise IncorrectParameterError(
                f'\nOperation: {self._operation_stack}.\n'
                f'The previous operation returned the string\n'
                f'"stop_all_further_operations_with_success"\n'
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
        self._set_branch_stack()
        if self._is_it_operation:
            self._current_operation._set_branch_stack(self._branch_stack)
            self._operation_stack = self._current_operation._update_stack()
        else:
            self._operation_stack = f"{self._branch_stack}(branch)"

        self._update_rw_inst(self._rw_inst_from_option)

        if input_data is None and self._def_args is not None:
            input_data = self._def_args
        elif input_data is None and self._def_args is None:
            self._end_branch_check()
            return None
        sd, self._rw_inst = ResultParser.sort_data(to_tuple(input_data), self._rw_inst)
        if sd.stop_all_further_operations_with_success:
            self._end_branch_check(
                sd.stop_all_further_operations_with_success)
            return None
        input_data = sd.data

        if self._is_it_operation:
            self._current_operation._update_rw_inst(self._rw_inst)
            # print("111op", self._operation_stack, self._rw_inst["val"].__dict__)
            # self._current_operation.renew_def_rw_inst()
            self._operation_stack = self._current_operation._get_op_stack()
            if self._current_operation._hide_init_inf_from_logs is None:
                self._current_operation._hide_init_inf_from_logs = self._hide_init_inf_from_logs

            result, rem_args = self._current_operation.run(input_data)
            print("Remaining_args/result", result, rem_args)

            if self._operations is None and self._delayed_return is not None:
                self._current_operation._stop_distribution = True

            if self._delayed_return is not None and not self._current_operation._stop_distribution or self._distribute_input_data:
                self._distribute_input_data = False
                self._current_operation._distribute_input_data = True

            if self._current_operation._distribute_input_data and not self._delayed_return:
                self._delayed_return = to_tuple(result)
                result = () if rem_args is None else rem_args
            elif self._delayed_return:
                self._delayed_return = (*self._delayed_return, *to_tuple(result))
                result = () if rem_args is None else rem_args
            if self._current_operation._stop_distribution:
                print(999999)
                result = self._delayed_return[0] if len(
                    self._delayed_return) == 1 else self._delayed_return
                self._delayed_return = None

            print(111111111, self._delayed_return, result)

            if self._operations is None:
                return result
            result = self.run(result)
            return result

        if self._current_operation._all_operations_must_be_executed is None:
            self._current_operation._all_operations_must_be_executed = self._all_operations_must_be_executed
        if self._current_operation._hide_init_inf_from_logs is None:
            self._current_operation._hide_init_inf_from_logs = self._hide_init_inf_from_logs
        self._current_operation._set_branch_stack(self._branch_stack)
        self._current_operation._set_rw_inst(self._rw_inst)
        self._current_operation.renew_def_rw_inst()

        result = self._current_operation.run(input_data)
        if self._operations is None:
            if self._assign is not None:
                kw = {key: self._rw_inst[key.split(".")[0]] for key in self._assign}
                return assign(*to_tuple(result), **kw)
            return result

        result = self.run(result)
        if self._assign is not None:
            kw = {key: self._rw_inst[key.split(".")[0]] for key in self._assign}
            return assign(*to_tuple(result), **kw)
        return result
