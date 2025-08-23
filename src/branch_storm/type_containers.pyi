from typing import Any, Type, TypeVar, Generic, overload, Union

T = TypeVar("T")

class _Mand(Generic[T]):
    # m[int]  -> T
    @overload
    def __getitem__(self, typ: Type[T]) -> T: ...
    @overload
    def __getitem__(self, typ: Any) -> Any: ...
    @overload
    def __call__(self,
                 link_or_pos: Union[int, str, Any] = ...,
                 seq: bool = ...) -> Any: ...
    @overload
    def __call__(self, *, seq: bool = ...) -> Any: ...

class _Opt(_Mand[T]): ...

MandatoryArgTypeContainer: _Mand[Any]
OptionalArgTypeContainer: _Opt[Any]
