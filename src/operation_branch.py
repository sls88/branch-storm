from typing import Any, Dict, Optional, Tuple, Union, Callable, Type


class Operation:
    def __init__(self,
                 class_or_func: Union[Callable, Type],
                 instance: Optional[Union[Any, str]] = None,
                 method: Optional[str] = None) -> None:
        self._class_or_func = class_or_func
        self._instance = instance
        self._method = method
        self._func_args_kwargs = None
        self._init_args_kwargs = None
        self._meth_args_kwargs = None
        self._op_name: Optional[str] = None
        self._rw_inst: Optional[Dict[str, Any]] = None
        self._def_args: Optional[Tuple] = None
        self._assign: Optional[Tuple[str]] = None
        self._distribute_result: bool = False
        self._stop_distribution: bool = False
        self._burn_rem_args: bool = False
        self._raise_err_if_empty_data: bool = False

    def func(self, *args, **kwargs) -> "Operation":
        self._func_args_kwargs = args, kwargs
        return self

    def init(self, *args, **kwargs) -> "Operation":
        self._init_args_kwargs = args, kwargs
        return self

    def meth(self, *args, **kwargs) -> "Operation":
        self._meth_args_kwargs = args, kwargs
        return self

    def op_name(self, name: str) -> "Operation":
        self._op_name = name
        return self

    def rw_inst(self, rw_inst: Dict[str, Any]) -> "Operation":
        self._rw_inst = rw_inst
        return self

    def def_args(self, *def_args: Tuple[Any, ...]) -> "Operation":
        self._def_args = def_args
        return self

    def assign(self, *args: Tuple[str, ...]) -> "Operation":
        self._assign = args
        return self

    @property
    def distribute_result(self) -> "Operation":
        self._distribute_result = True
        return self

    @property
    def stop_distribution(self) -> "Operation":
        self._stop_distribution = True
        return self

    @property
    def burn_rem_args(self) -> "Operation":
        self._burn_rem_args = True
        return self

    @property
    def raise_err_if_empty_data(self) -> "Operation":
        self._raise_err_if_empty_data = True
        return self


class Branch:
    def __init__(self,
                 *operations: Union[Operation, Callable, Type]) -> None:
        self._operations = operations
        self._br_name: Optional[str] = None
        self._rw_inst: Optional[Dict[str, Any]] = None
        self._assign: Optional[Tuple[str]] = None
        self._distribute_result: bool = False
        self._stop_distribution: bool = False
        self._burn_rem_args: bool = False
        self._raise_err_if_empty_data: bool = False

    def br_name(self, name: str) -> "Branch":
        self._br_name = name
        return self

    def rw_inst(self, rw_inst: Dict[str, Any]) -> "Branch":
        self._rw_inst = rw_inst
        return self

    def assign(self, *args: Tuple[str, ...]) -> "Branch":
        self._assign = args
        return self

    @property
    def distribute_result(self) -> "Branch":
        self._distribute_result = True
        return self

    @property
    def stop_distribution(self) -> "Branch":
        self._stop_distribution = True
        return self

    @property
    def burn_rem_args(self) -> "Branch":
        self._burn_rem_args = True
        return self

    @property
    def raise_err_if_empty_data(self) -> "Branch":
        self._raise_err_if_empty_data = True
        return self
