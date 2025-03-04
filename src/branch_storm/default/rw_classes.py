from dataclasses import dataclass, Field, field


@dataclass
class Values:
    """One time write then read only built-in immutable pos_args or pos_args structures:
    str, int, float, complex, tuple, range, frozenset, bool, bytes, bytearray, memoryview

    (tuple and frozenset can contain nested structures of each other's types)
    Writing other types will throw an exception.

    Purpose: 1 time write then only read
    """
    _op_stack_name: str = ""

    def __setattr__(self, key, value):
        if key in self.__dict__ and key != "_op_stack_name":
            start_mess = f"Operation: {self._op_stack_name}. " if self._op_stack_name else ""
            raise ValueError(f"{start_mess}The value cannot be overwritten. "
                             f"The class is intended for single-write and read use.")
        if isinstance(value, Field):
            dcl_field = value
            value = dcl_field.default
            self.__dataclass_fields__[key] = dcl_field
        else:
            self.__check_data_structure(value)
            self.__dataclass_fields__[key] = field(default=value)
        self.__dict__[key] = value

    def __check_data_structure(self, value) -> None:
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

        start_mess = f"Operation: {self._op_stack_name}. " if self._op_stack_name else ""
        raise TypeError(f"{start_mess}The pos_args or pos_args structure being written has types other than: "
                        f"str, int, float, complex, tuple, range, frozenset, "
                        f"bool, bytes, bytearray, memoryview.")

    def __getattribute__(self, item):
        if item in [
            '_Values__check_data_structure',
            '__annotations__', '__class__',
            '__dataclass_fields__', '__dataclass_params__',
            '__delattr__', '__dict__', '__dir__', '__doc__',
            '__eq__', '__format__', '__ge__', '__getattribute__',
            '__gt__', '__hash__', '__init__', '__init_subclass__',
            '__le__', '__lt__', '__module__', '__ne__', '__new__',
            '__reduce__', '__reduce_ex__', '__repr__', '__setattr__',
            '__sizeof__', '__str__', '__subclasshook__', '__weakref__',
            '_op_stack_name']:
            pass
        elif item not in self.__dict__:
            start_mess = f"Operation: {self._op_stack_name}. " if self._op_stack_name else ""
            raise AttributeError(f"{start_mess}No such attribute in Variables")
        return super().__getattribute__(item)


@dataclass
class Variables:
    """Write, rewrite and read any pos_args structures."""
    _op_stack_name: str = ""

    def __setattr__(self, key, value):
        if isinstance(value, Field):
            dcl_field = value
            value = dcl_field.default
            self.__dataclass_fields__[key] = dcl_field
        else:
            self.__dataclass_fields__[key] = field(default=value)
        self.__dict__[key] = value

    def __getattribute__(self, item):
        if item in [
            '__annotations__', '__class__', '__dataclass_fields__',
            '__dataclass_params__', '__delattr__', '__dict__', '__dir__',
            '__doc__', '__eq__', '__format__', '__ge__', '__getattribute__',
            '__gt__', '__hash__', '__init__', '__init_subclass__', '__le__',
            '__lt__', '__module__', '__ne__', '__new__', '__reduce__',
            '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__',
            '__subclasshook__', '__weakref__', '_op_stack_name']:
            pass
        elif item not in self.__dict__:
            start_mess = f"Operation: {self._op_stack_name}. " if self._op_stack_name else ""
            raise AttributeError(f"{start_mess}No such attribute in Variables")
        return super().__getattribute__(item)
