from typing import TypeVar

from typing_extensions import Generic, TypeVarTuple, Unpack

TT = TypeVarTuple('TT')
T = TypeVar('T')

class ArgTypeContainer(Generic[Unpack[TT]]):
    pass


class SeqIdenticalTypesContainer(Generic[T]):
    pass
