from inspect import Parameter
from typing import TypeVar, Union, Optional, Tuple, Generic


T = TypeVar('T')


class MandatoryArgTypeContainer(Generic[T]):
    def __init__(self, link_or_pos: Union[int, str] = None, seq: bool = False):
        self.link_or_pos = link_or_pos
        self.is_it_seq_ident_types = seq
        self.number_position: Optional[int] = None
        self.param_link: Optional[str] = None
        self._parse_link_or_pos()
        self.par_type = Parameter.empty
        self.par_value = Parameter.empty

    def _parse_link_or_pos(self) -> None:
        if isinstance(self.link_or_pos, str):
            self.param_link = self.link_or_pos
        elif isinstance(self.link_or_pos, int):
            self.number_position = self.link_or_pos

    def _validate(self) -> Optional[str]:
        if isinstance(self.par_type, Tuple):
            return f"Using a type tuple is not possible. Passed types: {self.par_type}"
        elif self.is_it_seq_ident_types and (self.number_position or self.param_link):
            return ("You cannot use seq=True with an object "
                    "reference parameter or an input data position index.")
        elif not (isinstance(self.link_or_pos, str) or isinstance(
                self.link_or_pos, int)) and not self.is_it_seq_ident_types:
            return "Cannot initialize a type container without specifying any parameters"
        elif self.number_position and self.number_position < 1:
            return "The number_position should start from 1"

    def __getitem__(self, par_type) -> "MandatoryArgTypeContainer":
        self.par_type = par_type
        return self


class OptionalArgTypeContainer(MandatoryArgTypeContainer, Generic[T]):
    pass
