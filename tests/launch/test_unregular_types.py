import pytest

from inspect import Parameter

import src.branch_storm.initialization_core as core


class _AmbiguousTruth:
    def __bool__(self):
        raise ValueError("Ambiguous truth value for comparison result")


class PandasLike:
    def __eq__(self, other):
        return _AmbiguousTruth()
    def __ne__(self, other):
        return _AmbiguousTruth()
    def __repr__(self):
        return "<PandasLike>"


class HardErrorCompare:
    def __eq__(self, other):
        raise TypeError("Equality not supported")
    def __ne__(self, other):
        raise TypeError("Inequality not supported")
    def __repr__(self):
        return "<HardErrorCompare>"

SENTINEL_EMPTY = Parameter.empty


class StubMandatoryTC:
    def __init__(self, number_position=None, is_it_seq_ident_types=False,
                 par_value=Parameter.empty, par_type=Parameter.empty):
        self.number_position = number_position
        self.is_it_seq_ident_types = is_it_seq_ident_types
        self.par_value = par_value
        self.par_type = par_type


@pytest.fixture(autouse=True)
def patch_is_it_init_arg_type(monkeypatch):
    monkeypatch.setattr(core, "is_it_init_arg_type",
                        lambda x: "mandatory" if isinstance(x, StubMandatoryTC) else None)
    yield


def test_expand_positions_with_pandaslike_positional_identity_filter_ok():
    obj = PandasLike()
    arg0 = StubMandatoryTC(number_position=1)
    st = core.InitState(
        stack="stack/pl/pos",
        params_wo_self={},
        args_in=(arg0,),
        kwargs_in={},
        input_data=(obj, "tail"),
    )
    out = core.ContainerExpander.expand_to_positions(st)
    assert isinstance(out.args_in[0].par_value, PandasLike)
    assert out.input_data == ("tail",)


def test_expand_positions_with_pandaslike_keyword_identity_filter_ok():
    obj = PandasLike()
    kw = StubMandatoryTC(number_position=1)
    st = core.InitState(
        stack="stack/pl/kw",
        params_wo_self={},
        args_in=(),
        kwargs_in={"k": kw},
        input_data=(obj, 123),
    )
    out = core.ContainerExpander.expand_to_positions(st)
    assert isinstance(out.kwargs_in["k"].par_value, PandasLike)
    assert out.input_data == (123,)


def test_expand_positions_out_of_range_mandatory_raises():
    arg0 = StubMandatoryTC(number_position=5)
    st = core.InitState(
        stack="stack/oob",
        params_wo_self={}, args_in=(arg0,), kwargs_in={}, input_data=("only",),
    )
    with pytest.raises(core.EmptyDataError):
        core.ContainerExpander.expand_to_positions(st)


def test_expand_positions_keyword_seq_ident_types_is_error():
    kw = StubMandatoryTC(number_position=None, is_it_seq_ident_types=True)
    st = core.InitState(
        stack="stack/seq/kw",
        params_wo_self={}, args_in=(), kwargs_in={"bad": kw}, input_data=(1,2,3),
    )
    with pytest.raises(TypeError):
        core.ContainerExpander.expand_to_positions(st)


def test_fill_params_identity_checks_with_pandaslike_in_par_value_and_empty_type():
    p = core.Param(type=StubMandatoryTC(par_value=PandasLike(), par_type=Parameter.empty))
    out = core.fill_params({"p": p})
    assert isinstance(out["p"].value, PandasLike)
    assert out["p"].type is Parameter.empty
    assert out["p"].arg is Parameter.empty


def test_fill_params_when_par_type_present_moves_to_arg():
    p = core.Param(type=StubMandatoryTC(par_value="raw", par_type=int))
    out = core.fill_params({"p": p})
    assert out["p"].arg == "raw"
    assert out["p"].type is int


def test_fill_def_values_with_pandaslike_value_no_ambiguity():
    p = core.Param(
        arg=PandasLike(), type=int, value=Parameter.empty, kind="POSITIONAL_ONLY",
        def_val=Parameter.empty, type_container="optional"
    )
    errs, out = core.fill_def_values({"p": p})
    assert errs == {}
    assert out["p"].value is Parameter.empty
    assert isinstance(out["p"].arg, PandasLike)


def test_fill_def_values_optional_with_default_applies_value():
    p = core.Param(
        arg=Parameter.empty, type=int, value=Parameter.empty, kind="KEYWORD_ONLY",
        def_val=777, type_container="optional"
    )
    errs, out = core.fill_def_values({"p": p})
    assert errs == {}
    assert out["p"].value == 777
    assert out["p"].type is Parameter.empty


def test_fill_def_values_optional_without_default_collects_error():
    p = core.Param(
        arg=Parameter.empty, type=int, value=Parameter.empty, kind="POSITIONAL_ONLY",
        def_val=Parameter.empty, type_container="optional"
    )
    errs, _ = core.fill_def_values({"p": p})
    assert errs == {"p": "optional"}


def test_check_arg_type_ignores_when_arg_is_empty():
    p = core.Param(arg=Parameter.empty, type=int)
    errs = core.check_arg_type({"p": p}, check_type_strategy_all=True)
    assert errs == {}


def test_fill_values_moves_arg_to_value_only_when_arg_is_not_empty():
    p = core.Param(arg=Parameter.empty, value=Parameter.empty)
    out = core.fill_values({"p": p})
    assert out["p"].value is Parameter.empty
    p2 = core.Param(arg="x", value=Parameter.empty)
    out2 = core.fill_values({"p": p2})
    assert out2["p"].value == "x"


def test_sequence_consumer_with_type_stops_on_nonmatching_pandaslike_and_restores_input():
    obj = PandasLike()
    input_data, pmap, _ = core.SequenceConsumer.consume_seq_with_type(
        input_data=(1, 2, obj, 99),
        new_param_map={}, kind="POSITIONAL_ONLY",
        type_container="mandatory", seq_num=0, a_type=int
    )
    assert isinstance(input_data[0], PandasLike)
    assert input_data[1] == 99
    assert len(pmap) == 2


def test_shape_validator_detects_missing_positional():
    st = core.InitState(
        stack="stack/shape", params_wo_self={},
        args_in=(), kwargs_in={}, input_data=(),
    )
    st.arg_params = {"x": core.Param(kind="POSITIONAL_ONLY", def_val=Parameter.empty)}
    st.kw_params = {}
    with pytest.raises(ValueError):
        core.ShapeValidator.check_lengths(st)
