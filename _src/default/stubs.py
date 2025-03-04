from typing import Any, Optional


def get_all_args_return_default_value(*args, def_value: Optional[Any]) -> Optional[Any]:
    return def_value


def raise_err_if_none_received(data: Optional[Any], error: Any) -> Any:
    if data is None:
        raise error
    return data
