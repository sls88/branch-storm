from dataclasses import dataclass
from inspect import Parameter
from typing import Any, Optional, Tuple, Dict


@dataclass
class Param:
    arg: Any = Parameter.empty
    type: Any = Parameter.empty
    value: Any = Parameter.empty
    kind: str = Parameter.empty
    def_val: Any = Parameter.empty
    type_container: str = Parameter.empty
