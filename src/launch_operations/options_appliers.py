from typing import Any, Dict, List, Optional, Tuple, Union

from src.dataclasses import LaunchParameters, Flags, Configurations
from src.launch_operations.errors import AssignmentError, ArgumentError
from src.launch_operations.rw_inst_updater import RwInstUpdater
from src.launch_operations.launch_utils import get_stack_name
from src.operation import Operation, Branch


class OperationOptionsApplier:
    def __init__(self,
                 param: LaunchParameters,
                 conf: Configurations) -> None:
        self._param = param
        self._conf = conf
        self._operation = self._param.current_operation
        self._op_name = None

    def _get_flags(self) -> None:
        self._param.flags.stop_distribution = self._operation._stop_distribution
        self._param.flags.distribute_result = self._operation._distribute_input_data
        self._param.flags.burn_rem_args = self._operation._burn_rem_args
        self._param.flags.raise_err_if_empty_data = self._operation._raise_err_if_empty_data
        self._param.flags.all_operations_must_be_executed = self._conf.all_operations_must_be_executed
        self._param.flags.hide_init_inf_from_logs = self._conf.hide_init_inf_from_logs

    def _get_def_args(self, init_data: Optional[Any],
                      distribute_args: Optional[Any]) -> None:
        op_def = self._operation._def_args
        return () if not isinstance(operations, LaunchParameters) and \
                     init_data is None and \
                     def_args is None and \
                     distribute_args is None else def_args

    def _get_current_operation(self):
        pass

    def apply_options(self, init_data: Optional[Any],
                      distribute_args: Optional[Any]) -> LaunchParameters:
        if isinstance(self._operation)
        self._get_def_args(init_data, distribute_args)
        self._op_name = self._operation._op_name
        OptionsChecker.check_name(self._op_name, self._param.last_op_name)
        self._param.def_args =
        self._param.fields_for_assign = self._operation._assign
        self._get_flags()
        stack_name = get_stack_name(par.name_stack, self._op_name)
        OptionsChecker.check_burn_rem_args(self._param.flags, stack_name)
        self._param.rw_inst = RwInstUpdater.get_updated(
            stack_name, self._operation._rw_inst, self._rw_inst)
        self._param.current_operation = self._operation
        self._param.fields_for_assign = self._operation._fields_for_assign
        OptionsChecker.check_assign_option(self._param.fields_for_assign, self._rw_inst, stack_name)

        return self._param


class BranchOptionsApplier:
    def __init__(self, param: LaunchParameters) -> None:
        self._param = param
        self._branch = self._param.branch_operations
        self._current_operation = None
        self._def_args = None

    def _get_current_operation(self) -> None:
        if self._branch._operations:
            self._current_operation = self._branch._operations[0]
            if len(self._branch._operations) == 1:
                self._branch._operations = None

    def _get_base_branch_stack(self):
        br_name = self._branch._br_name
        if self._param.base_branch_stack == "INITIAL RUN":
            self._param.base_branch_stack = br_name
        else:
            self._param.base_branch_stack = \
                f"{self._param.base_branch_stack} -> {br_name}"

    def _get_def_args(self, init_data: Optional[Any],
                      distribute_args: Optional[Any]) -> None:
        if self._param.base_branch_stack == "INITIAL RUN" and init_data is None and \
                distribute_args is None and self._branch._def_args is None:
            self._def_args = ()
        else:
            self._def_args = self._branch._def_args

    def apply_options(self, init_data: Optional[Any],
                      distribute_args: Optional[Any]
                      ) -> Tuple[LaunchParameters, Union[Branch, Operation], Optional[Tuple]]:
        self._get_current_operation()
        OptionsChecker.check_op_or_br(self._current_operation, self._param.last_op_stack)
        self._get_def_args(init_data, distribute_args)
        OptionsChecker.check_name(self._branch._br_name, self._param.last_op_stack)
        self._get_base_branch_stack()
        self._param.rw_inst = RwInstUpdater.get_updated(
            self._param.last_op_stack, self._param.rw_inst, self._branch._rw_inst)

        return self._param, self._current_operation, self._def_args


class OptionsChecker:
    @staticmethod
    def check_name(name: Optional[str], last_op_stack: str) -> None:
        if name is not None and not isinstance(name, str):
            raise TypeError(f"The last successful operation: {last_op_stack}. "
                            f"The name passed in the option "
                            f"must be in string format.")

    @staticmethod
    def check_assign_option(fields_for_assign: Optional[Tuple[str, ...]],
                            rw_inst: Dict[str, Any],
                            stack_name: str) -> None:
        if fields_for_assign is not None:
            if not all(map(lambda x: isinstance(x, str), fields_for_assign)):
                raise TypeError(f"Stack: {stack_name}. All values to assign must be string only.")
            aliases = list(map(lambda x: x.split(".")[0], fields_for_assign))
            for alias in aliases:
                if alias not in rw_inst:
                    raise AssignmentError(f"Stack: {stack_name}. Alias \"{alias}\" "
                                          f"is missing from rw_inst. Assignment not possible.")

    @staticmethod
    def check_burn_rem_args(flags: Flags, stack_name: str) -> None:
        if flags.burn_rem_args and flags.distribute_result:
            raise ArgumentError(f"Stack: {stack_name}. It is not possible to simultaneously "
                                f"burn the remaining arguments and distribute the result.")

    @staticmethod
    def check_op_or_br(operation: Union[Operation, Branch], last_op_stack: str) -> None:
        if operation is None:
            raise TypeError(f"The last successful operation: {last_op_stack}. "
                            f"A branch must have at least one operation.")
        elif not isinstance(operation, Operation) or not isinstance(operation, Branch):
            raise TypeError(f"The last successful operation: {last_op_stack}. "
                            f"Passed object must be Operation or Branch. "
                            f"Passed entity: {operation}.")
