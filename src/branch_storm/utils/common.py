from typing import Any, Dict, Optional, Type, Tuple



def extract_attrpath(x) -> Optional[str]:
    if x is None:
        return None

    if getattr(x, "__bs_is_attrproxy__", False):
        p = getattr(x, "path", None)
        if isinstance(p, str):
            return p

    for name in (
    "__bs_attrpath__", "__bs_attr_path__", "_bs_attrpath", "_bs_attr_path",
    "__attrpath__", "attr_path", "path"):
        if hasattr(x, name):
            val = getattr(x, name)
            if callable(val):
                try:
                    val = val()
                except TypeError:
                    pass
            if isinstance(val, str):
                return val

    return None


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


def to_tuple(data: Any) -> Tuple:
    return (data,) if not isinstance(data, Tuple) else data


def unwrap_single_tuple(data: Any) -> Any:
    """If given a single-element tuple, unwraps and returns its content.
    Otherwise, returns the original object unchanged."""
    return data[0] if isinstance(data, tuple) and len(data) == 1 else data
