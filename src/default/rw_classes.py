from dataclasses import dataclass, Field, field
from typing import NoReturn


@dataclass
class Values:
    """One time write then read only built-in immutable pos_args or pos_args structures:
    str, int, float, complex, tuple, range, frozenset, bool, bytes, bytearray, memoryview

    (tuple and frozenset can contain nested structures of each other's types)
    Writing other types will throw an exception.

    Purpose: 1 time write then only read
    """
    def __setattr__(self, key, value):
        if key in self.__dict__:
            raise ValueError("The value cannot be overwritten. "
                             "The class is intended for single-write and read use.")
        if isinstance(value, Field):
            dcl_field = value
            value = dcl_field.default
            self.__dataclass_fields__[key] = dcl_field
        else:
            self.__check_data_structure(value)
            self.__dataclass_fields__[key] = field(default=value)
        self.__dict__[key] = value

    def __check_data_structure(self, value) -> NoReturn:
        if any([isinstance(value, frozenset),
                isinstance(value, tuple)]):
            for pos in value:
                self.__check_data_structure(pos)
            return None
        elif any([isinstance(value, str),
                  isinstance(value, int),
                  isinstance(value, float),
                  isinstance(value, complex),
                  isinstance(value, range),
                  isinstance(value, bool),
                  isinstance(value, bytes),
                  isinstance(value, bytearray),
                  isinstance(value, memoryview)]):
            return None
        raise TypeError("The pos_args or pos_args structure being written has types other than: "
                        "str, int, float, complex, tuple, range, frozenset, "
                        "bool, bytes, bytearray, memoryview.")


@dataclass
class Variables:
    """Write, rewrite and read any pos_args structures."""
    def __setattr__(self, key, value):
        if isinstance(value, Field):
            dcl_field = value
            value = dcl_field.default
            self.__dataclass_fields__[key] = dcl_field
        else:
            self.__dataclass_fields__[key] = field(default=value)
        self.__dict__[key] = value
