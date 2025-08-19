import uuid
from dataclasses import dataclass, field
from inspect import Parameter
from typing import Any, Callable, Dict, Optional, Sequence, Tuple, Type, Union

from typeguard import check_type, TypeCheckError, CollectionCheckStrategy

from .launch_operations.errors import EmptyDataError
from .type_containers import MandatoryArgTypeContainer, OptionalArgTypeContainer
from .utils.formatters import LoggerBuilder


log = LoggerBuilder().build()


KwargsDict = Dict[str, Any]

ArgTypeContainer = Union[
    Type[Union[MandatoryArgTypeContainer, OptionalArgTypeContainer]],
    MandatoryArgTypeContainer,
    OptionalArgTypeContainer,
]


@dataclass
class Param:
    """Internal holder for parameter metadata/value during binding."""
    arg: Any = Parameter.empty
    type: Any = Parameter.empty
    value: Any = Parameter.empty
    kind: str = Parameter.empty
    def_val: Any = Parameter.empty
    type_container: str = Parameter.empty


@dataclass
class InitState:
    """Mutable pipeline state used by steps while building args/kwargs."""
    stack: str
    params_wo_self: Dict[str, Parameter]
    args_in: Tuple
    kwargs_in: KwargsDict
    input_data: Tuple
    check_type_strategy_all: bool = True
    arg_params: Dict[Union[str, float, int], Param] = field(
        default_factory=dict)
    kw_params: Dict[str, Param] = field(default_factory=dict)
    rem_data: Optional[Tuple] = None


@dataclass
class ResultState:
    """Final materialized values returned by the pipeline."""
    args: Tuple
    kwargs: KwargsDict
    rem_data: Optional[Tuple]


class TypeContainerValidator:
    """Validates type containers passed in args/kwargs before any processing."""
    @staticmethod
    def validate(st: InitState) -> InitState:
        err_args: Dict[int, str] = {}
        for num, arg in enumerate(st.args_in, 1):
            if is_it_init_arg_type(arg):
                validation_res = arg._validate()
                if validation_res:
                    err_args[num] = validation_res

        err_kwargs: Dict[str, str] = {}
        for name, value in st.kwargs_in.items():
            if is_it_init_arg_type(value):
                validation_res = value._validate()
                if validation_res:
                    err_kwargs[name] = validation_res

        if err_args or err_kwargs:
            raise ValueError(
                f"Operation: {st.stack}. There was found incorrect type_containers.\n"
                f"Len: {len({**err_args, **err_kwargs})}; kwargs: {err_kwargs},\n"
                f"where key = argument name, value = error message;\n"
                f"args: {err_args}, where key = argument number position,\n"
                f"value = error message."
            )
        return st


class ContainerExpander:
    """Expands containers that reference input_data positions for args/kwargs."""
    @staticmethod
    def expand_to_positions(st: InitState) -> InitState:
        len_inp_data = len(st.input_data)
        unique_id = str(uuid.uuid4())
        args_not_enough: Dict[int, int] = {}
        new_args = []
        for num, arg in enumerate(st.args_in, 1):
            type_container = is_it_init_arg_type(arg)
            if type_container and getattr(arg, "number_position", None):
                elem, st.input_data = replace_and_get_elem_by_pos(
                    st.input_data, arg.number_position, unique_id
                )
                if elem == Parameter.empty and type_container == "mandatory":
                    args_not_enough[num] = arg.number_position
                arg.par_value = elem
            new_args.append(arg)
        st.args_in = tuple(new_args)

        if args_not_enough:
            raise EmptyDataError(
                f"Operation: {st.stack}\n"
                f"For mandatory positional arguments, "
                f"the position numbers of the input data were declared,\n"
                f"but there was not enough data. Len: {len(args_not_enough)},\n"
                f"Position arguments: {args_not_enough},\n"
                f"where key = ordinal number of argument, "
                f"value = declared position.\n"
                f"Total length of input tuple: {len_inp_data}."
            )

        kwargs_not_enough: Dict[str, int] = {}
        seq_for_kwargs = []
        for name, arg in st.kwargs_in.items():
            type_container = is_it_init_arg_type(arg)
            if not type_container:
                continue

            if getattr(arg, "number_position", None):
                elem, st.input_data = replace_and_get_elem_by_pos(
                    st.input_data, arg.number_position, unique_id)
                if elem == Parameter.empty and type_container == "mandatory":
                    kwargs_not_enough[name] = arg.number_position
                arg.par_value = elem
                st.kwargs_in[name] = arg
            elif getattr(arg, "is_it_seq_ident_types", False):
                seq_for_kwargs.append(name)

        if kwargs_not_enough:
            raise EmptyDataError(
                f"Operation: {st.stack}\n"
                f"For mandatory keyword arguments, "
                f"the position numbers of the input data were declared,\n"
                f"but there was not enough data. "
                f"Len: {len(kwargs_not_enough)},\n"
                f"Keyword arguments: {kwargs_not_enough},\n"
                f"where key = argument name, value = declared position.\n"
                f"Total length of input data tuple: {len_inp_data}.")

        if seq_for_kwargs:
            raise TypeError(
                f"Operation: {st.stack}\n"
                f"Keyword arguments were found that attempted to assign a\n"
                f"sequence of identical types. Len: {len(seq_for_kwargs)}\n"
                f"Arguments names: {seq_for_kwargs}\n"
                f"A keyword argument can only have one type. Sequences can\n"
                f"only be passed for positional arguments. "
                f"Set seq=False (default)")

        st.input_data = tuple(x for x in st.input_data if x != unique_id)
        return st


class ParamMapBuilder:
    """Builds internal param maps (positional/keyword) from signature parameters."""

    @staticmethod
    def build(st: InitState) -> InitState:
        params: Dict[str, Param] = {}
        for name, p in list(st.params_wo_self.items()):
            params[name] = Param(kind=p.kind.name, def_val=p.default)

        kw_flag = False
        for name, param in params.items():
            if name in st.kwargs_in:
                kw_flag = True
            if kw_flag and param.kind != "VAR_KEYWORD":
                param.kind = "KEYWORD_ONLY"

        arg_params: Dict[str, Param] = {}
        kw_params: Dict[str, Param] = {}
        for name, param in params.items():
            if param.kind in ("KEYWORD_ONLY", "VAR_KEYWORD"):
                kw_params[name] = param
            elif param.kind == "VAR_POSITIONAL":
                arg_params[name] = param
            else:
                param.kind = "POSITIONAL_ONLY"
                arg_params[name] = param

        st.arg_params, st.kw_params = arg_params, kw_params
        return st


class ShapeValidator:
    """Validates shape: missing mandatory args/kwargs and unexpected leftovers."""
    @staticmethod
    def check_lengths(st: InitState) -> InitState:
        args = st.args_in
        var_positional = any(p.kind == "VAR_POSITIONAL" for
                             p in st.arg_params.values())

        mand_args_not_enough = []
        for name, param in st.arg_params.items():
            if param.kind == "POSITIONAL_ONLY":
                if param.def_val == Parameter.empty and not args:
                    mand_args_not_enough.append(name)
                _, args = get_first_element(args)

        args_types = tuple(type(arg) for arg in args)
        if not var_positional and args:
            raise ValueError(
                f"Operation: {st.stack}. There was found "
                f"args not used in call/init. "
                f"Len: {len(args)}; args: {args_types}"
            )
        if mand_args_not_enough:
            raise ValueError(
                f"Operation: {st.stack}. Mandatory args were "
                f"found that were not passed to the call/init. "
                f"Len {len(mand_args_not_enough)}; "
                f"arg names: {mand_args_not_enough}"
            )

        var_keyword = any(
            p.kind == "VAR_KEYWORD" for p in st.kw_params.values())
        not_used_kwargs = {name: val for name, val in st.kwargs_in.items()
                           if name not in st.kw_params and not var_keyword}
        if not_used_kwargs:
            hidden = {n: type(v) for n, v in not_used_kwargs.items()}
            raise ValueError(
                f"Operation: {st.stack}. There was found "
                f"kwargs not used in call/init. "
                f"Len: {len(not_used_kwargs)}; "
                f"kwargs: {hidden}")

        mand_kwargs_not_enough = [
            name for name, param in st.kw_params.items()
            if (param.kind == "KEYWORD_ONLY" and param.def_val ==
                Parameter.empty and name not in st.kwargs_in)]

        if mand_kwargs_not_enough:
            raise ValueError(
                f"Operation: {st.stack}. Mandatory kwargs were "
                f"found that were not passed to the call/init. "
                f"Len {len(mand_kwargs_not_enough)}; "
                f"kwargs names: {mand_kwargs_not_enough}")

        return st


class TypeValueAttacher:
    """Attaches (type, value, container-kind) info from passed args/kwargs."""
    @staticmethod
    def attach(st: InitState) -> InitState:
        arg_params = {n: a for n, a in st.arg_params.items() if
                      a.kind != "VAR_POSITIONAL"}
        args = st.args_in
        for name, par in arg_params.items():
            elem, args = get_first_element(args)
            type_cont = is_it_arg_type(elem) or Parameter.empty
            el_type, value = get_type_value(elem)
            par.type, par.value, par.type_container = el_type, value, type_cont
        counter = 1
        while args:
            elem, args = get_first_element(args)
            type_cont = is_it_arg_type(elem) or Parameter.empty
            el_type, value = get_type_value(elem)
            arg_params[f"{counter}_pos_arg"] = Param(
                type=el_type, value=value,
                kind="VAR_POSITIONAL", type_container=type_cont)
            counter += 1
        st.arg_params = arg_params

        kw_params = {n: a for n, a in st.kw_params.items() if
                     a.kind != "VAR_KEYWORD"}
        for name, val in st.kwargs_in.items():
            type_cont = is_it_arg_type(val) or Parameter.empty
            el_type, value = get_type_value(val)
            if name in kw_params:
                p = kw_params[name]
                p.type, p.value, p.type_container = el_type, value, type_cont
            else:
                kw_params[name] = Param(
                    type=el_type, value=value,
                    kind="KEYWORD_ONLY", type_container=type_cont)
        st.kw_params = kw_params

        return st


class RulesValidator:
    """Validates special rules: sequences-only-for-varpositional and mandatory-after-optional order."""
    @staticmethod
    def check_sequences_and_order(st: InitState) -> InitState:
        seq_for_args = []
        for name, param in st.arg_params.items():
            type_container = is_it_init_arg_type(param.type)
            if (type_container and param.type.is_it_seq_ident_types and
                    param.kind != "VAR_POSITIONAL"):
                seq_for_args.append(name)
        if seq_for_args:
            raise TypeError(
                f"Operation: {st.stack}\n"
                f"Positional arguments were found "
                f"that attempted to assign a\n"
                f"sequence of identical types. "
                f"Len: {len(seq_for_args)}\n"
                f"Arguments names: {seq_for_args}\n"
                f"Only var_positional arguments can "
                f"consume sequences of input data.\n"
                f"Set seq=False (default)"
            )

        err_containers: Dict[Union[str, int], str] = {}
        params_all = {**st.arg_params, **st.kw_params}
        opt_flag = False
        for name, param in params_all.items():
            if param.type_container == "optional":
                opt_flag = True
            if param.type_container == "mandatory" and opt_flag:
                err_containers[name] = param.type_container
        if err_containers:
            raise TypeError(
                f"Operation: {st.stack}. Len {len(err_containers)}, "
                f"Args map: {err_containers}\n"
                f"A container for the type of a mandatory argument "
                f"cannot be passed after an optional one.\n")
        return st


class InitContainerFiller:
    """Resolves values stored inside init-type containers (par_value/par_type)."""
    @staticmethod
    def fill(st: InitState) -> InitState:
        st.arg_params = fill_params(st.arg_params)
        st.kw_params = fill_params(st.kw_params)
        return st


class Binder:
    """Binds input_data items to parameters according to containers and sequence flags."""
    @staticmethod
    def bind_input_data(st: InitState) -> InitState:
        seq_num: Union[int, float] = 0
        new_map: Dict[Union[str, float, int], Param] = {}
        kind = "POSITIONAL_ONLY"
        input_data = st.input_data

        for name, param in st.arg_params.items():
            if param.kind == "VAR_POSITIONAL":
                seq_num = int(seq_num + 1)
                name = seq_num

            type_container = is_it_arg_type(param.type)
            if type_container:
                a_type = get_args_from_arg_type(param.type)
                if (a_type and is_it_init_arg_type(param.type) and
                        param.type.is_it_seq_ident_types):
                    input_data, p_map, seq_num = (
                        SequenceConsumer.consume_seq_with_type(
                        input_data, new_map, kind,
                            type_container, seq_num, a_type
                    ))
                    new_map.update(p_map)
                elif (not a_type and is_it_init_arg_type(param.type) and
                      param.type.is_it_seq_ident_types):
                    input_data, p_map, seq_num = (
                        SequenceConsumer.consume_seq_without_type(
                        input_data, new_map, kind, type_container, seq_num
                    ))
                    new_map.update(p_map)
                else:
                    param, input_data = set_arg_type_value(
                        param, input_data, a_type)
                    new_map[name] = param
            else:
                new_map[name] = param

        st.input_data = input_data
        st.arg_params = new_map

        for name, param in st.kw_params.items():
            type_container = is_it_arg_type(param.type)
            if type_container:
                a_type = get_args_from_arg_type(param.type)
                param, st.input_data = set_arg_type_value(
                    param, st.input_data, a_type)
                st.kw_params[name] = param

        st.rem_data = st.input_data if st.input_data else None
        return st


class DefaultsResolver:
    """Fills defaults for optional arguments or raises an error if data is insufficient."""
    @staticmethod
    def resolve_or_raise(st: InitState) -> InitState:
        not_args, st.arg_params = fill_def_values(st.arg_params)
        not_kw, st.kw_params = fill_def_values(st.kw_params)
        not_enough = {**not_args, **not_kw}

        if not not_enough:
            return st

        raise TypeError(
            f"Operation: {st.stack}. "
            f"Len: {len(not_enough)}, Args map: {not_enough}.\n"
            f"For the listed arguments you expected to receive data of the\n "
            f"corresponding types, but apparently they were not enough for\n"
            f"the call/initialization.\n"
            f"Maybe type container does not match the expected argument.\n"
            f"If the argument is optional, then if it is not received,\n"
            f"the function must have a default value for it.\n"
            f"If the key is of type int this means the position number of\n"
            f"the positional arguments in *args tuple"
        )


class TypeChecker:
    """Checks runtime types according to containers and materializes values for args/kwargs."""
    @staticmethod
    def check(st: InitState) -> InitState:
        for _, param in st.arg_params.items():
            if (param.value == Parameter.empty and
                    param.def_val != Parameter.empty):
                param.value = param.def_val

        kw_type_err = check_arg_type(
            st.kw_params, st.check_type_strategy_all)
        args_type_err = check_arg_type(
            st.arg_params, st.check_type_strategy_all)
        if kw_type_err or args_type_err:
            common_map = {**args_type_err, **kw_type_err}
            raise TypeError(
                f"Operation: {st.stack}.\n"
                f"Argument mismatches with their types were found:\n"
                f"Len: {len(common_map)}; Arg type map: {common_map}\n"
                f"where dict(argument_name: tuple(actual_arg_type, "
                f"expected_arg_type))\n"
                f"If the argument name is of type int this means the "
                f"positional arguments position number in *args tuple"
            )

        st.arg_params = fill_values(st.arg_params)
        st.kw_params = fill_values(st.kw_params)
        return st


class Materializer:
    """Produces final args/kwargs/rem_data tuples from state maps."""

    @staticmethod
    def materialize(st: InitState) -> ResultState:
        args = tuple(param.value for param in st.arg_params.values())
        kwargs = {name: param.value for name, param in st.kw_params.items()}
        rem = None if not st.rem_data else st.rem_data
        return ResultState(args=args, kwargs=kwargs, rem_data=rem)


StepFn = Callable[[InitState], InitState]
MaterializeFn = Callable[[InitState], ResultState]


class StepRegistry:
    """Holds the default InitState -> InitState pipeline and a materializer."""
    _pipeline: Tuple[StepFn, ...] = (
        TypeContainerValidator.validate,
        ContainerExpander.expand_to_positions,
        ParamMapBuilder.build,
        ShapeValidator.check_lengths,
        TypeValueAttacher.attach,
        RulesValidator.check_sequences_and_order,
        InitContainerFiller.fill,
        Binder.bind_input_data,
        DefaultsResolver.resolve_or_raise,
        TypeChecker.check,
    )
    _materializer: MaterializeFn = Materializer.materialize

    @classmethod
    def get_pipeline(cls) -> Tuple[StepFn, ...]:
        return cls._pipeline

    @classmethod
    def set_pipeline(cls, steps: Sequence[StepFn]) -> None:
        cls._pipeline = tuple(steps)

    @classmethod
    def reset_default(cls) -> None:
        cls._pipeline = (
            TypeContainerValidator.validate,
            ContainerExpander.expand_to_positions,
            ParamMapBuilder.build,
            ShapeValidator.check_lengths,
            TypeValueAttacher.attach,
            RulesValidator.check_sequences_and_order,
            InitContainerFiller.fill,
            Binder.bind_input_data,
            DefaultsResolver.resolve_or_raise,
            TypeChecker.check,
        )
        cls._materializer = Materializer.materialize

    @classmethod
    def get_materializer(cls) -> MaterializeFn:
        return cls._materializer

    @classmethod
    def set_materializer(cls, fn: MaterializeFn) -> None:
        cls._materializer = fn


class PipelineRunner:
    """Executes the configured steps and materializes the result."""
    @staticmethod
    def run(
        st: InitState,
        pipeline: Optional[Sequence[StepFn]] = None,
        materializer: Optional[MaterializeFn] = None,
    ) -> ResultState:
        steps = tuple(pipeline) if pipeline is not None \
            else StepRegistry.get_pipeline()
        for step in steps:
            st = step(st)
        mat = materializer or StepRegistry.get_materializer()
        return mat(st)

class InitCore:
    """Builds args/kwargs for call/init based on signature, input_data and type containers."""
    @staticmethod
    def get_args_kwargs(
        stack: str,
        params_wo_self: Dict[str, Parameter],
        args: Tuple,
        kwargs: KwargsDict,
        input_data: Tuple,
        check_type_strategy_all: bool = True,
        *,
        pipeline: Optional[Sequence[StepFn]] = None,
        materializer: Optional[MaterializeFn] = None,
    ) -> Tuple[Tuple, KwargsDict, Optional[Tuple]]:
        st = InitState(
            stack=stack,
            params_wo_self=params_wo_self,
            args_in=args,
            kwargs_in=kwargs,
            input_data=input_data,
            check_type_strategy_all=check_type_strategy_all,
        )
        res = PipelineRunner.run(st, pipeline=pipeline, materializer=materializer)
        return res.args, res.kwargs, res.rem_data


class SequenceConsumer:
    """Consumes sequences from input_data into positional parameters."""

    @staticmethod
    def consume_seq_with_type(
        input_data: Tuple,
        new_param_map: Dict[Union[str, float, int], Param],
        kind: str,
        type_container: str,
        seq_num: Union[int, float],
        a_type: Type,
    ) -> Tuple[Tuple, Dict[Union[str, float, int], Param],
                Union[str, float, int]]:
        execution_flag = False
        while True:
            elem, input_data = get_first_element(input_data)
            strategy = CollectionCheckStrategy.ALL_ITEMS
            try:
                check_type(elem, a_type, collection_check_strategy=strategy)
                execution_flag = True
            except TypeCheckError:
                break
            seq_num = round(seq_num + 0.1, 3)
            new_param_map[seq_num] = Param(
                value=elem, kind=kind, type_container=type_container)

        if not execution_flag:
            seq_num = int(seq_num)
            new_param_map[seq_num] = Param(
                arg=Parameter.empty, type=a_type,
                kind=kind, type_container=type_container)

        elem = () if elem == Parameter.empty else (elem,)
        input_data = (*elem, *input_data)
        return input_data, new_param_map, seq_num

    @staticmethod
    def consume_seq_without_type(
        input_data: Tuple,
        new_param_map: Dict[Union[str, float, int], Param],
        kind: str,
        type_container: str,
        seq_num: Union[int, float],
    ) -> Tuple[Tuple, Dict[Union[str, float, int], Param],
                Union[str, float, int]]:
        while input_data:
            elem, input_data = get_first_element(input_data)
            param = Param(value=elem, kind=kind,
                          type_container=type_container)
            seq_num = round(seq_num + 0.1, 3)
            new_param_map[seq_num] = param
        return input_data, new_param_map, seq_num


def set_arg_type_value(
        param: Param,
        input_data: Tuple,
        a_type: Type) -> Tuple[Param, Tuple]:
    elem, input_data = get_first_element(input_data)
    if a_type:
        param.arg = elem
        param.type = a_type
    else:
        param.arg = Parameter.empty
        param.type = Parameter.empty
        param.value = elem
    return param, input_data


def fill_params(params: Dict[Union[str, float, int], Param]
                ) -> Dict[Union[str, float, int], Param]:
    for name, param in params.items():
        if is_it_init_arg_type(param.type):
            if param.type.par_value != Parameter.empty:
                if param.type.par_type == Parameter.empty:
                    param.value = param.type.par_value
                    param.type = Parameter.empty
                else:
                    param.arg = param.type.par_value
                    param.type = param.type.par_type
                params[name] = param
    return params


def fill_def_values(
        params: Dict[Union[str, float, int], Param]
) -> Tuple[Dict[Union[str, int], str], Dict[Union[str, float, int], Param]]:
    args_not_enough: Dict[Union[str, int], str] = {}
    new_params: Dict[Union[str, float, int], Param] = {}
    for name, param in params.items():
        arg_empty_cond = (param.type != Parameter.empty and
                          param.arg == Parameter.empty)
        if (arg_empty_cond and param.type_container == "optional" and
                param.def_val != Parameter.empty):
            param.value = param.def_val
            param.type = Parameter.empty
            new_params[name] = param
        elif (arg_empty_cond and param.type_container == "optional" and
              param.kind == "KEYWORD_ONLY"):
            continue
        elif (arg_empty_cond and param.type_container == "optional" and
              param.kind == "VAR_POSITIONAL"):
            continue
        elif ((arg_empty_cond and param.type_container == "optional" and
               param.def_val == Parameter.empty) or
              (arg_empty_cond and param.type_container == "mandatory")):
            args_not_enough[name] = param.type_container
        elif (param.value == Parameter.empty and
              param.def_val != Parameter.empty):
            param.value = param.def_val
        new_params[name] = param
    return args_not_enough, new_params


def is_it_arg_type(arg: Any) -> Optional[str]:
    if "__dict__" in dir(arg) and "__origin__" in arg.__dict__ and \
            arg.__dict__["__origin__"] is OptionalArgTypeContainer or \
            isinstance(arg, OptionalArgTypeContainer):
        return "optional"
    elif "__dict__" in dir(arg) and "__origin__" in arg.__dict__ and \
            arg.__dict__["__origin__"] is MandatoryArgTypeContainer or \
            isinstance(arg, MandatoryArgTypeContainer):
        return "mandatory"
    class_name = None
    try:
        class_name = arg.__name__
    except AttributeError:
        pass
    if class_name == "MandatoryArgTypeContainer":
        return "mandatory"
    elif class_name == "OptionalArgTypeContainer":
        return "optional"


def is_it_init_arg_type(arg: Any) -> Optional[str]:
    if isinstance(arg, OptionalArgTypeContainer):
        return "optional"
    elif isinstance(arg, MandatoryArgTypeContainer):
        return "mandatory"


def get_args_from_arg_type(
        type_container: ArgTypeContainer) -> Optional[Type]:
    if isinstance(type_container, MandatoryArgTypeContainer):
        if type_container.par_type == Parameter.empty:
            return None
        return type_container.par_type
    if "__args__" in type_container.__dict__:
        return type_container.__dict__["__args__"][0]
    return None


def get_first_element(input_data: Tuple) -> Tuple[Any, Tuple]:
    if not len(input_data):
        return Parameter.empty, input_data
    return input_data[0], input_data[1:]


def replace_and_get_elem_by_pos(
        input_data: Tuple,
        elem_pos: int,
        replacement: Any) -> Tuple[Any, Tuple]:
    if elem_pos <= 0 or elem_pos > len(input_data):
        return Parameter.empty, input_data
    elem = input_data[elem_pos - 1]
    input_data = list(input_data)
    input_data[elem_pos - 1] = replacement
    return elem, tuple(input_data)


def get_type_value(elem: Any) -> Tuple[Type, Any]:
    if is_it_arg_type(elem):
        return elem, Parameter.empty
    return Parameter.empty, elem


def check_arg_type(
        params: Dict[Union[str, float, int], Param],
        check_type_strategy_all: bool = True
) -> Dict[Union[str, int], Tuple]:
    type_err: Dict[Union[str, int], Tuple] = {}
    for name, param in params.items():
        if param.arg != Parameter.empty:
            strategy = (
                CollectionCheckStrategy.ALL_ITEMS) if (
                check_type_strategy_all) else (
                CollectionCheckStrategy.FIRST_ITEM)
            try:
                check_type(param.arg, param.type,
                           collection_check_strategy=strategy)
            except TypeCheckError:
                type_err[name] = (type(param.arg), param.type)
    return type_err


def fill_values(
        params: Dict[Union[str, float, int], Param]
) -> Dict[Union[str, float, int], Param]:
    for name, param in params.items():
        if param.arg != Parameter.empty:
            param.value = param.arg
            params[name] = param
    return params
