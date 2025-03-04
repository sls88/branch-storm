from dataclasses import dataclass
from typing import Any, List, Optional, Tuple, Callable, Dict

from ..constants import STOP_CONSTANT


@dataclass
class SortedData:
    data: Tuple
    rw_instances: Optional[List[Any]] = None
    stop_all_operations: bool = False

    def separate_rw_instances(self, rw_inst: Dict[str, Any]) -> None:
        result = ResultParser.split_data_by_condition(
            self.data, rw_inst, ResultParser.is_it_rw_instance)
        self.data, self.rw_instances = result

    def separate_all_operations_constant(self, rw_inst: Dict[str, Any]) -> None:
        result = ResultParser.split_data_by_condition(
            self.data, rw_inst, ResultParser.is_it_stop_message)
        self.data, stop_all_operations = result

        if stop_all_operations:
            self.stop_all_operations = True

    def update_instances(self, rw_inst: Dict[str, Any]) -> Dict[str, Any]:
        """Add rw_instance in List if not exist or reassign existed."""
        for new_instance in self.rw_instances:
            for name, old_rw in rw_inst.items():
                if isinstance(new_instance, type(old_rw)):
                    rw_inst[name] = new_instance

        return rw_inst


class ResultParser:
    @staticmethod
    def sort_data(data: Tuple, rw_inst: Dict[str, Any]) -> Tuple[SortedData, Dict[str, Any]]:
        """Separate the positions corresponding to the passed conditions from the received pos_args.
        Replace the rw_inst instances with new ones if exist or add if not exist.

        If such an instance is removed from a tuple of 2 elements, the tuple will be unpacked.

        If the returned pos_args contains only an instance of the run class, it will be reassigned.
        This will also be a trigger to start the next operation without passing pos_args to it.
        """
        sd = SortedData(data=data)
        sd.separate_rw_instances(rw_inst)
        sd.separate_all_operations_constant(rw_inst)
        new_rw_inst = sd.update_instances(rw_inst)

        return sd, new_rw_inst

    @staticmethod
    def split_data_by_condition(data: Tuple, rw_inst: Dict[str, Any], condition: Callable) -> Tuple[Tuple, List[Any]]:
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
    def is_it_stop_message(obj: Any, _: Dict[str, Any]) -> bool:
        """Check if the obj is a stop message: "stop_all_further_operations_with_success_result".

        Return True if yes.
        """
        return isinstance(obj, str) and obj == STOP_CONSTANT
