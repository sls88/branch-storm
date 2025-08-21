from typing import Any, Tuple, Union

from src.branch_storm.operation import (
    CallChain, ChainStep, StepKind, CallChainPreviewer
)


def make_chain(
    target: Any,
    steps: Tuple[ChainStep, ...],
    *,
    user_defined_op_name: Union[str, None] = None,
) -> CallChain:
    return CallChain(
        target=target,
        steps=steps,
        user_defined_op_name=user_defined_op_name,
    )


def dummy_func(a: int) -> int:
    return a


class BuildUser:
    def __init__(self, id: int) -> None:
        self.id = id

    def set_name(self, name: str) -> "BuildUser":
        return self

    def save(self) -> None:
        pass


class WithProp:
    def __init__(self, x: int) -> None:
        self.x = x
        self.container = {"svc": self}

    def method3(self) -> int:
        return 3


def test_canonical_first_call_with_lookahead_getitem():
    """Flow -> WithProp(*int).container["svc"].method3(?)"""
    steps = (
        ChainStep(kind=StepKind.CALL),
        ChainStep(kind=StepKind.ATTR, name="container"),
        ChainStep(kind=StepKind.GETITEM, key="svc"),
        ChainStep(kind=StepKind.ATTR, name="method3"),
        ChainStep(kind=StepKind.CALL),
    )
    chain = make_chain(WithProp, steps)
    previewer = CallChainPreviewer(br_stack="Flow", chain=chain)

    line = previewer.format_line(cur_idx=0, current_args=(123,), current_kwargs={})
    assert line == 'Flow -> WithProp(*(int,)).container["svc"].method3(?)'


def test_canonical_second_call_past_collapsed():
    steps = (
        ChainStep(kind=StepKind.CALL, args=("captured",)),
        ChainStep(kind=StepKind.ATTR, name="set_name"),
        ChainStep(kind=StepKind.CALL),
    )
    chain = make_chain(BuildUser, steps)
    previewer = CallChainPreviewer(br_stack="Job", chain=chain)

    line = previewer.format_line(cur_idx=1, current_args=("Alex",),
                                 current_kwargs={})
    assert line == "Job -> BuildUser(*args).set_name(*(str,))"


def test_user_defined_name_overrides_canonical_small_chain():
    steps = (ChainStep(kind=StepKind.CALL),)
    chain = make_chain(BuildUser, steps, user_defined_op_name="CreateUser")
    previewer = CallChainPreviewer(br_stack="Flow", chain=chain)

    line = previewer.format_line(cur_idx=0, current_args=(), current_kwargs={"id": 10})
    assert line == "Flow -> CreateUser (**{id: int})"


def test_compression_of_repeated_types_in_args():
    steps = (ChainStep(kind=StepKind.CALL),)
    chain = make_chain(dummy_func, steps)
    previewer = CallChainPreviewer(br_stack="F", chain=chain)

    line = previewer.format_line(cur_idx=0, current_args=(1, 2, 3, 4), current_kwargs={})
    assert line == "F -> dummy_func(*(int, ...x4))"


def test_single_arg_trailing_comma_kept():
    steps = (ChainStep(kind=StepKind.CALL),)
    chain = make_chain(dummy_func, steps)
    previewer = CallChainPreviewer(br_stack="F", chain=chain)

    line = previewer.format_line(cur_idx=0, current_args=(42,), current_kwargs={})
    assert line == "F -> dummy_func(*(int,))"


def test_kwargs_rendering_types():
    steps = (ChainStep(kind=StepKind.CALL),)
    chain = make_chain(dummy_func, steps)
    previewer = CallChainPreviewer(br_stack="F", chain=chain)

    line = previewer.format_line(cur_idx=0, current_args=(), current_kwargs={"name": "ann", "age": 5})
    assert line.startswith("F -> dummy_func(")
    assert "**{name: str" in line or "**{age: int" in line
    assert "age: int}" in line or "name: str}" in line
    assert line.endswith(")")


def test_multiple_steps_with_getitem_in_lookahead_and_past_shown():
    steps = (
        ChainStep(kind=StepKind.CALL),
        ChainStep(kind=StepKind.ATTR, name="container"),
        ChainStep(kind=StepKind.GETITEM, key=0),
        ChainStep(kind=StepKind.ATTR, name="svc"),
        ChainStep(kind=StepKind.GETITEM, key="x"),
        ChainStep(kind=StepKind.ATTR, name="method3"),
        ChainStep(kind=StepKind.CALL),
    )
    chain = make_chain(WithProp, steps)
    previewer = CallChainPreviewer(br_stack="Pipe", chain=chain)

    line = previewer.format_line(cur_idx=0, current_args=(7,), current_kwargs={})
    assert line == 'Pipe -> WithProp(*(int,)).container[0].svc["x"].method3(?)'


def test_last_call_shows_past_getitem_and_w_o_args():
    steps = (
        ChainStep(kind=StepKind.CALL, args=("arg",)),
        ChainStep(kind=StepKind.ATTR, name="container"),
        ChainStep(kind=StepKind.GETITEM, key="svc"),
        ChainStep(kind=StepKind.ATTR, name="method3"),
        ChainStep(kind=StepKind.CALL),
    )
    chain = make_chain(WithProp, steps)
    previewer = CallChainPreviewer(br_stack="Pipe", chain=chain)

    line = previewer.format_line(cur_idx=1, current_args=(), current_kwargs={})
    assert line == 'Pipe -> WithProp(*args).container["svc"].method3(w/o args)'


def test_all_future_calls_marked_question():
    steps = (
        ChainStep(kind=StepKind.CALL),
        ChainStep(kind=StepKind.ATTR, name="a"),
        ChainStep(kind=StepKind.CALL),
        ChainStep(kind=StepKind.ATTR, name="b"),
        ChainStep(kind=StepKind.CALL),
    )
    chain = make_chain(WithProp, steps)
    previewer = CallChainPreviewer(br_stack="Flow", chain=chain)

    line = previewer.format_line(cur_idx=0, current_args=(1,), current_kwargs={})
    assert line == "Flow -> WithProp(*(int,)).a(?).b(?)"


def test_mixed_args_kwargs_current_call():
    steps = (
        ChainStep(kind=StepKind.CALL),
    )
    chain = make_chain(dummy_func, steps)
    previewer = CallChainPreviewer(br_stack="F", chain=chain)

    line = previewer.format_line(cur_idx=0, current_args=(1, 2), current_kwargs={"x": "a"})
    assert line == "F -> dummy_func(*(int, ...x2), **{x: str})"


def test_user_defined_name_deeper_chain():
    steps = (
        ChainStep(kind=StepKind.CALL),
        ChainStep(kind=StepKind.ATTR, name="save"),
        ChainStep(kind=StepKind.CALL),
    )
    chain = make_chain(BuildUser, steps, user_defined_op_name="SaveUser")
    previewer = CallChainPreviewer(br_stack="Job", chain=chain)

    line = previewer.format_line(cur_idx=1, current_args=(), current_kwargs={})
    assert line == "Job -> SaveUser (w/o args)"


def test_slice_getitem_in_preview_future_and_past():
    sl = slice(1, 10, 2)
    steps = (
        ChainStep(kind=StepKind.CALL),
        ChainStep(kind=StepKind.ATTR, name="container"),
        ChainStep(kind=StepKind.GETITEM, key=sl),
        ChainStep(kind=StepKind.ATTR, name="method3"),
        ChainStep(kind=StepKind.CALL),
        ChainStep(kind=StepKind.ATTR, name="container"),
        ChainStep(kind=StepKind.GETITEM, key=0),
        ChainStep(kind=StepKind.ATTR, name="method3"),
        ChainStep(kind=StepKind.CALL),
    )
    chain = make_chain(WithProp, steps)
    previewer = CallChainPreviewer(br_stack="Pipe", chain=chain)

    line = previewer.format_line(cur_idx=0, current_args=(9,), current_kwargs={})
    assert line == "Pipe -> WithProp(*(int,)).container[1:10:2].method3(?).container[0].method3(?)"


def test_no_args_current_call_w_o_args():
    steps = (ChainStep(kind=StepKind.CALL),)
    chain = make_chain(BuildUser, steps)
    previewer = CallChainPreviewer(br_stack="Flow", chain=chain)

    line = previewer.format_line(cur_idx=0, current_args=(), current_kwargs={})
    assert line == "Flow -> BuildUser(w/o args)"


def test_future_calls_only_question_even_if_getitem_between():
    steps = (
        ChainStep(kind=StepKind.CALL),
        ChainStep(kind=StepKind.ATTR, name="container"),
        ChainStep(kind=StepKind.GETITEM, key="svc"),
        ChainStep(kind=StepKind.ATTR, name="method3"),
        ChainStep(kind=StepKind.CALL),
        ChainStep(kind=StepKind.ATTR, name="container"),
        ChainStep(kind=StepKind.GETITEM, key=0),
        ChainStep(kind=StepKind.ATTR, name="method3"),
        ChainStep(kind=StepKind.CALL),
    )
    chain = make_chain(WithProp, steps)
    previewer = CallChainPreviewer(br_stack="Flow", chain=chain)

    line = previewer.format_line(cur_idx=0, current_args=(5,), current_kwargs={})
    assert line == 'Flow -> WithProp(*(int,)).container["svc"].method3(?).container[0].method3(?)'
