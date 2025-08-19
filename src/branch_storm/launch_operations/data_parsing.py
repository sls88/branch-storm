from dataclasses import dataclass
from typing import Any, List, Optional, Tuple, Callable, Dict

from ..constants import STOP_CONSTANT, SKIP_OPERATION_CONSTANT
from ..default.rw_classes import RunConfigurations
from ..utils.common import unwrap_single_tuple, to_tuple


@dataclass
class SortedData:
    data: Tuple
    rw_instances: Optional[List[Any]] = None
    stop_constant: bool = False
    skip_operation_constant: bool = False

    def separate_rw_instances(self, rw_inst: Dict[str, Any]) -> None:
        result = ResultParser.split_data_by_condition(
            self.data, rw_inst, ResultParser.is_it_rw_instance)
        self.data, self.rw_instances = result

    def separate_all_operations_constant(self, rw_inst: Dict[str, Any]) -> None:
        result = ResultParser.split_data_by_condition(
            self.data, rw_inst, ResultParser.is_it_stop_message)
        self.data, stop_constant = result

        if stop_constant:
            self.stop_constant = True

        result = ResultParser.split_data_by_condition(
            self.data, rw_inst, ResultParser.is_it_skip_operation_constant)
        self.data, second_stop_constant = result

        if second_stop_constant:
            self.skip_operation_constant = True

    def separate_run_conf(self) -> None:
        result = ResultParser.split_data_by_condition(
            self.data, {}, ResultParser.is_it_run_conf)
        self.data, self.rw_instances = result

    def update_instances(self, rw_inst: Dict[str, Any]) -> Dict[str, Any]:
        """Add rw_instance in List if not exist or reassign existed."""
        for new_instance in self.rw_instances:
            if isinstance(new_instance, RunConfigurations):
                continue
            for name, old_rw in rw_inst.items():
                if isinstance(new_instance, type(old_rw)):
                    rw_inst[name] = new_instance

        return rw_inst


class ResultParser:
    @staticmethod
    def sort_data(data: Tuple, rw_inst: Dict[str, Any]
                  ) -> Tuple[SortedData, Dict[str, Any]]:
        """Separate the positions corresponding to the passed conditions from the received pos_args.
        Replace the rw_inst instances with new ones if exist or add if not exist.

        If such an instance is removed from a tuple of 2 elements, the tuple will be unpacked.

        If the returned pos_args contains only an instance of the run class, it will be reassigned.
        This will also be a trigger to start the next operation without passing pos_args to it.
        """
        data_type = type(data)
        data = to_tuple(data)
        sd = SortedData(data=data)
        sd.separate_rw_instances(rw_inst)
        sd.separate_all_operations_constant(rw_inst)
        new_rw_inst = sd.update_instances(rw_inst)
        if not data_type == tuple:
            sd.data = unwrap_single_tuple(sd.data)
        return sd, new_rw_inst

    @staticmethod
    def separate_run_conf(data: Tuple) -> Tuple[Any, RunConfigurations]:
        """Separate RunConfigurations instance from data and unpack 1 len tuple"""
        sd = SortedData(data=data)
        sd.separate_run_conf()
        data = unwrap_single_tuple(sd.data)
        inst = sd.rw_instances
        return data, inst[0] if inst else None

    @staticmethod
    def find_run_conf_from_rw_inst(
            rw_inst: Optional[Tuple[Dict[str, Any]]]) -> Optional[RunConfigurations]:
        if rw_inst:
            run_configs = []
            for one_block in rw_inst:
                for inst in one_block.values():
                    if isinstance(inst, RunConfigurations):
                        run_configs.append(inst)
            if run_configs:
                return run_configs.pop()

    @staticmethod
    def split_data_by_condition(
            data: Tuple,
            rw_inst: Dict[str, Any],
            condition: Callable) -> Tuple[Tuple, List[Any]]:
        """Perform pos_args analysis and separate special elements from tuple sequence only.

        (separate elements, corresponding passed conditions from
        received pos_args and return it as tuple[pos_args, list[elements/instances]]).
        """
        instances, new_data = [], []
        for num, pos in enumerate(data):
            if condition(pos, rw_inst):
                instances += [pos]
            else:
                new_data.append(pos)
        return tuple(new_data), instances

    @staticmethod
    def is_it_rw_instance(obj: Any, rw_inst: Dict[str, Any]) -> bool:
        """Check if the obj is a rw_instance.

        Return True if yes.
        """
        rw_classes = list(map(lambda x: type(x), rw_inst.values()))
        return any(map(lambda dclss: isinstance(obj, dclss), rw_classes))

    @staticmethod
    def is_it_run_conf(obj: Any, _: Dict[str, Any]) -> bool:
        """Check if the obj is a RunConfigurations.

        Return True if yes.
        """
        return isinstance(obj, RunConfigurations)

    @staticmethod
    def is_it_stop_message(obj: Any, _: Dict[str, Any]) -> bool:
        """Check if the obj is a stop message: "stop_all_further_operations_with_success_result".

        Return True if yes.
        """
        return isinstance(obj, str) and obj == STOP_CONSTANT

    @staticmethod
    def is_it_skip_operation_constant(obj: Any, _: Dict[str, Any]) -> bool:
        """Check if the obj is a stop message: "internal_skip_operation_constant".

        Return True if yes.
        """
        return isinstance(obj, str) and obj == SKIP_OPERATION_CONSTANT
