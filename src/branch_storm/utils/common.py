from typing import Any, Dict, Optional, Type, Tuple

from ..default.rw_classes import Values, Variables


def find_rw_inst(string: str, rw_inst: Dict[str, Any]) -> Optional[Type]:
    """Return a special class if the string parameter is equal its alias.

    rw_inst = {"ja": JobArgs, "ac": AnotherClass}
    string = "ja" -> JobArgs()

    rw_inst = {"ac": AnotherClass}
    string = "ja" -> None
    """
    for alias in rw_inst:
        if string == alias:
            return rw_inst[alias]


def renew_instance(
        stack: str,
        old_rw_inst: Dict[str, Any],
        rw_class: Type) -> Dict[str, Any]:
    for alias, rw_inst in old_rw_inst.items():
        if isinstance(rw_inst, rw_class):
            new_values = rw_class()
            new_values._op_stack_name = stack
            for field, value in rw_inst.__dict__.items():
                setattr(new_values, field, value)
            return {alias: new_values}


def renew_def_rw_inst(stack: str, rw_inst: Dict[str, Any]) -> Dict[str, Any]:
    if rw_inst:
        return {**rw_inst, **renew_instance(stack, rw_inst, Values),
                **renew_instance(stack, rw_inst, Variables)}
    return rw_inst


def to_tuple(data: Any) -> Tuple:
    return (data,) if not isinstance(data, Tuple) else data
