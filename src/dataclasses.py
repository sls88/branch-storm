from dataclasses import dataclass
from inspect import Parameter
from typing import Any, Optional, List, Tuple, Union


@dataclass
class Param:
    arg: Any = Parameter.empty
    type: Any = Parameter.empty
    value: Any = Parameter.empty
    kind: Optional[str] = Parameter.empty
    def_val: Optional[Any] = Parameter.empty


@dataclass
class Flags:
    all_operations_must_be_executed: bool = False
    is_it_initial_run: bool = False
    stop_distribution: bool = False
    distribute_result: bool = False
    burn_rem_args: bool = False
    raise_err_if_empty_data: bool = False


@dataclass
class LParameters:
    operations: Union[Any, List[Any]] = None
    name_stack: Optional[str] = None
    last_op_name: str = "INITIAL RUN"
    one_operation: Any = None
    many_operations: List[Any] = None
    remaining_operations: List[Any] = None
    delayed_return: Optional[Tuple] = None
    fields_for_assign: Optional[Tuple[str, ...]] = None
    flags: Flags = None

    def __post_init__(self):
        self.flags = Flags()
