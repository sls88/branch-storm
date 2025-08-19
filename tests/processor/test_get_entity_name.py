from dataclasses import dataclass
from typing import Any, Tuple, Union

from src.branch_storm.operation import CallObject, CallChain, ChainStep, StepKind


def dummy_func(a: int) -> int:
    return a


class WithProp:
    def __init__(self, x: int) -> None:
        self.x = x
        self.container = {"svc": self}

    def method3(self) -> int:
        return 3

    @property
    def prop(self) -> int:
        return 1


class Plain:
    def save(self) -> None:
        pass

    def do(self) -> None:
        pass


def make_chain(
    target: Any,
    steps: Tuple[ChainStep, ...],
    *,
    base_is_instance: bool = False,
    resolved_instance_cls_name: Union[str, None] = None,
    unresolved_external_path: Union[str, None] = None,
    user_defined_op_name: Union[str, None] = None,
) -> CallChain:
    return CallChain(
        target=target,
        steps=steps,
        base_is_instance=base_is_instance,
        resolved_instance_cls_name=resolved_instance_cls_name,
        unresolved_external_path=unresolved_external_path,
        user_defined_op_name=user_defined_op_name
    )


def test_entity_name_function_base():
    """Base is a function, no steps."""
    co = CallObject(dummy_func)
    co._chain = make_chain(dummy_func, ())
    assert co._call_chain.get_op_name() == "dummy_func"


def test_entity_name_function_with_attr_and_call():
    """func.attr() -> name with ATTR and CALL."""
    steps = (
        ChainStep(kind=StepKind.ATTR, name="attr"),
        ChainStep(kind=StepKind.CALL)
    )
    co = CallObject(dummy_func)
    co._chain = make_chain(dummy_func, steps)
    assert co._call_chain.get_op_name() == "dummy_func.attr()"


def test_entity_name_class_with_call_and_method_call():
    """Class() then .method3() -> 'WithProp().method3()'."""
    steps = (
        ChainStep(kind=StepKind.CALL),
        ChainStep(kind=StepKind.ATTR, name="method3"),
        ChainStep(kind=StepKind.CALL),
    )
    co = CallObject(WithProp)
    co._chain = make_chain(WithProp, steps)
    assert co._call_chain.get_op_name() == "WithProp().method3()"


def test_entity_name_class_with_getitem_and_method_call_str_key():
    """WithProp().container['svc'].method3() -> string key quoted."""
    steps = (
        ChainStep(kind=StepKind.CALL),
        ChainStep(kind=StepKind.ATTR, name="container"),
        ChainStep(kind=StepKind.GETITEM, key="svc"),
        ChainStep(kind=StepKind.ATTR, name="method3"),
        ChainStep(kind=StepKind.CALL),
    )
    co = CallObject(WithProp)
    co._chain = make_chain(WithProp, steps)
    assert co._call_chain.get_op_name() == 'WithProp().container["svc"].method3()'


def test_entity_name_getitem_int_and_slice():
    """Check integer and slice rendering in GETITEM."""
    # [0]
    steps_0 = (
        ChainStep(kind=StepKind.CALL),
        ChainStep(kind=StepKind.ATTR, name="container"),
        ChainStep(kind=StepKind.GETITEM, key=0),
    )
    co0 = CallObject(WithProp)
    co0._chain = make_chain(WithProp, steps_0)
    assert co0._call_chain.get_op_name() == "WithProp().container[0]"

    # [1:10]
    sl = slice(1, 10, None)
    steps_slice = (
        ChainStep(kind=StepKind.CALL),
        ChainStep(kind=StepKind.ATTR, name="container"),
        ChainStep(kind=StepKind.GETITEM, key=sl),
    )
    co1 = CallObject(WithProp)
    co1._chain = make_chain(WithProp, steps_slice)
    assert co1._call_chain.get_op_name() == "WithProp().container[1:10]"

    # [1:10:2]
    sl2 = slice(1, 10, 2)
    steps_slice2 = (
        ChainStep(kind=StepKind.CALL),
        ChainStep(kind=StepKind.ATTR, name="container"),
        ChainStep(kind=StepKind.GETITEM, key=sl2),
    )
    co2 = CallObject(WithProp)
    co2._chain = make_chain(WithProp, steps_slice2)
    assert co2._call_chain.get_op_name() == "WithProp().container[1:10:2]"


def test_entity_name_string_target_with_attr_and_call():
    target = "external.alias"
    steps = (
        ChainStep(kind=StepKind.ATTR, name="do"),
        ChainStep(kind=StepKind.CALL),
    )
    co = CallObject(target)
    co._chain = make_chain(target, steps, unresolved_external_path=target)
    assert (co._call_chain.get_op_name() ==
            'Instance from string: "external.alias".do()')


def test_entity_name_external_instance_suffix():
    """Already-initialized external instance should get '(instance)' suffix on base."""
    inst = Plain()
    steps = (
        ChainStep(kind=StepKind.ATTR, name="save"),
        ChainStep(kind=StepKind.CALL),
    )
    co = CallObject(inst)
    co._chain = make_chain(
        inst,
        steps,
        base_is_instance=True,
        resolved_instance_cls_name="Plain",
    )
    assert co._call_chain.get_op_name() == "Plain(instance).save()"


def test_entity_name_complex_chain_mixed():
    """
    Class().container['svc'].method3().prop -> ensure full canonical composition.
    Note: property access is ATTR (no CALL), so ends without '()'.
    """
    steps = (
        ChainStep(kind=StepKind.CALL),                     # Class()
        ChainStep(kind=StepKind.ATTR, name="container"),   # .container
        ChainStep(kind=StepKind.GETITEM, key="svc"),       # ["svc"]
        ChainStep(kind=StepKind.ATTR, name="method3"),     # .method3
        ChainStep(kind=StepKind.CALL),                     # ()
        ChainStep(kind=StepKind.ATTR, name="prop"),        # .prop
    )
    co = CallObject(WithProp)
    co._chain = make_chain(WithProp, steps)
    assert co._call_chain.get_op_name() == 'WithProp().container["svc"].method3().prop'


def test_get_entity_name_external_instance_from_string_method():
    class ManyRunMethods:
        def method3(self):
            pass

    @dataclass
    class InstanceStorage:
        many_meth_inst: ManyRunMethods = ManyRunMethods()

    co = CallObject("is.many_meth_inst").method3()
    co._get_instance_from_string("stack", {"is": InstanceStorage()})

    name = co._call_chain.get_op_name()
    assert name == "ManyRunMethods(instance).method3()"


def test_user_defined_name_overrides_small_chain():
    steps = (
        ChainStep(kind=StepKind.CALL),                     # WithProp()
        ChainStep(kind=StepKind.ATTR, name="method3"),     # .method3
        ChainStep(kind=StepKind.CALL),                     # ()
    )
    co = CallObject(WithProp)
    co._chain = make_chain(WithProp, steps, user_defined_op_name="CustomOp")
    assert co._call_chain.get_op_name() == "CustomOp"
