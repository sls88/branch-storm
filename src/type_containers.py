from typing import TypeVar

from typing_extensions import Generic, TypeVarTuple, Unpack

TT = TypeVarTuple('TT')
T = TypeVar('T')

class MandatoryArgTypeContainer(Generic[Unpack[TT]]):
    pass


class OptionalArgTypeContainer(Generic[Unpack[TT]]):
    pass


class SeqIdenticalTypesContainer(Generic[T]):
    pass
