from typing import Dict

import pytest

from src.branch_storm.operation import Operation as op, CallObject as obj
from src.branch_storm.type_containers import MandatoryArgTypeContainer as m


class ManyRunMethods:
    @staticmethod
    def method1():
        return 1

    @staticmethod
    def method2():
        return 2

    def method3(self):
        return 3


def factory_return_instance(x: int) -> ManyRunMethods:
    return ManyRunMethods()


def factory_dict() -> Dict[str, ManyRunMethods]:
    return {"svc": ManyRunMethods()}


def factory_nested():
    return {"list": [ManyRunMethods(), 123], "dict": {"svc": ManyRunMethods()}}


class WithProp:
    def __init__(self, x: int):
        self._container = {"svc": ManyRunMethods()}

    @property
    def container(self):
        return self._container


def test_chain_function_returns_instance_then_method():
    """
    func -> (returns instance) -> .method()
    """
    operation = op(obj(factory_return_instance)(m[int]).method3())
    res = operation.run((42, 7))

    assert res == (3, (7,))
    assert operation._opts.op_name == "factory_return_instance().method3()"


def test_chain_function_returns_dict_then_getitem_then_method():
    """
    func -> {} -> ["svc"] -> .method()
    """
    operation = op(obj(factory_dict)()["svc"].method3())
    res = operation.run()

    assert res == (3, None)
    assert operation._opts.op_name == 'factory_dict()["svc"].method3()'


def test_chain_function_returns_nested_dict_list_then_getitem_then_method():
    """
    func -> {"list":[ManyRunMethods(), ...]} -> ["list"][0] -> .method()
    """
    operation = op(obj(factory_nested)()["list"][0].method3())
    res = operation.run()

    assert res == (3, None)
    assert operation._opts.op_name == 'factory_nested()["list"][0].method3()'


def test_chain_class_init_then_property_then_getitem_then_method():
    """
    Class(...) -> .property -> ["svc"] -> .method()
    """
    operation = op(obj(WithProp)(m[int]).container["svc"].method3())
    res = operation.run((5,))

    assert res == (3, None)
    assert operation._opts.op_name == 'WithProp().container["svc"].method3()'


def test_chain_function_returns_dict_then_getitem_get_then_method():
    """
    func -> {"dict":{"svc": ManyRunMethods()}} -> ["dict"].get("svc") -> .method()
    """
    operation = op(obj(factory_nested)()["dict"].get("svc").method3())
    res = operation.run()

    assert res == (3, None)
    assert operation._opts.op_name == 'factory_nested()["dict"].get().method3()'


def test_getitem_misuse_with_operations_tuple_raises_typeerror_with_stack():
    def f() -> Dict[str, int]:
        return {"x": 1}

    bad_key = (op(obj(f)()),)

    operation = op(obj(f)()[bad_key])
    with pytest.raises(
            TypeError, match="Looks like branch/operations "
                             "tuple was used in item access"):
        operation.run()


def test_getitem_missing_key_propagates_keyerror():
    def f() -> Dict[str, int]:
        return {"x": 1}

    operation = op(obj(f)()["nope"])
    with pytest.raises(KeyError):
        operation.run()


def test_getitem_index_out_of_range_propagates_indexerror():
    def g():
        return [10]

    operation = op(obj(g)()[5])
    with pytest.raises(IndexError):
        operation.run()


def test_name_policy_function_only_call():
    def foo(a: int) -> int:
        return a

    operation = op(obj(foo)(m[int]))
    res = operation.run((10,))
    assert res == (10, None)
    assert operation._opts.op_name == "foo()"


def test_name_policy_class_then_method():
    class C:
        def __init__(self, x: int):
            self.x = x

        def m(self) -> int:
            return self.x

    operation = op(obj(C)(m[int]).m())
    res = operation.run((7,))
    assert res == (7, None)
    assert operation._opts.op_name == "C().m()"


def test_name_policy_function_then_instance_method():
    class Svc:
        def run(self):
            return 123

    def make_svc() -> Svc:
        return Svc()

    operation = op(obj(make_svc)().run())
    res = operation.run()
    assert res == (123, None)
    assert operation._opts.op_name == "make_svc().run()"
