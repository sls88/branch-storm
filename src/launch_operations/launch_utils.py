from typing import Any, Optional, Tuple, Dict, List, Union


def to_tuple(data: Any) -> Tuple:
    return (data,) if not isinstance(data, Tuple) else data
