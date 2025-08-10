from typing import Any, Dict, Optional, Tuple

from ..launch_operations.errors import IncorrectParameterError, AssignmentError, DistributionError


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
            stack: str,
            burn_rem_args: bool,
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
