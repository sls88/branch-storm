from abc import ABC
from dataclasses import dataclass, Field, field
from typing import Optional, Tuple, Any, Dict, List, Type, Callable, Union

from ..constants import INITIAL_RUN, DEFAULT_BRANCH_OPTIONS, INITIAL
from ..utils.options_utils import OptionsChecker


_ALLOWED_SCALARS = (str, int, float, complex, range,
                    bool, bytes, bytearray, memoryview)

def _is_dunder(name: str) -> bool:
    return name.startswith("__") and name.endswith("__")


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
            start_mess = f"Operation: {self._op_stack_name}. " if \
            self._op_stack_name else ""
            raise ValueError(
                f"{start_mess}The value cannot be overwritten. "
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
        if isinstance(value, (frozenset, tuple)):
            for pos in value:
                self.__check_data_structure(pos)
            return None
        if isinstance(value, _ALLOWED_SCALARS):
            return None
        start_mess = f"Operation: {self._op_stack_name}. " if \
            self._op_stack_name else ""
        raise TypeError(
            f"{start_mess}The pos_args or pos_args "
            f"structure being written has types other than: "
            f"str, int, float, complex, tuple, range, frozenset, "
            f"bool, bytes, bytearray, memoryview.")

    def __getattribute__(self, item):
        if item in ("_op_stack_name",
                    "_Values__check_data_structure") or _is_dunder(item):
            return super().__getattribute__(item)

        dct = super().__getattribute__("__dict__")
        if item not in dct:
            op = dct.get("_op_stack_name", "")
            start = f"Operation: {op}. " if op else ""
            raise AttributeError(f"{start}No such attribute in Values")

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
        if item == "_op_stack_name" or _is_dunder(item):
            return super().__getattribute__(item)

        dct = super().__getattribute__("__dict__")
        if item not in dct:
            op = dct.get("_op_stack_name", "")
            start = f"Operation: {op}. " if op else ""
            raise AttributeError(f"{start}No such attribute in Variables")

        return super().__getattribute__(item)


class RwInstUpdater:
    @staticmethod
    def get_updated_all(
        stack: str,
        current_rw_inst: Optional[Dict[str, Any]],
        rw_inst_from_option: Tuple[Dict[str, Any], ...]
    ) -> Dict[str, Any]:
        if rw_inst_from_option == ():
            current_rw_inst = RwInstUpdater._get_updated(
                stack, current_rw_inst, None)
        for rw_inst in rw_inst_from_option:
            current_rw_inst = RwInstUpdater._get_updated(
                stack, current_rw_inst, rw_inst)
        return current_rw_inst

    @staticmethod
    def _get_updated(
        stack: str,
        current_rw_inst: Optional[Dict[str, Any]],
        rw_inst_from_option: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        base_opt = rw_inst_from_option
        current = {} if current_rw_inst is None else dict(current_rw_inst)
        patch = {} if rw_inst_from_option is None else dict(rw_inst_from_option)

        RwInstUpdater._validate_rw_inst(stack, current, patch)

        current, patch = RwInstUpdater._do_commands(current, patch)

        updated = RwInstUpdater._merge_rw_inst(current, patch)

        if base_opt == {}:
            run_conf = updated.get("run_conf")
            run_conf_dct = {"run_conf": run_conf} if run_conf else {}
            updated = {
                **RwInstUpdater._separate_cls_inst(updated, Values),
                **RwInstUpdater._separate_cls_inst(updated, Variables),
                **run_conf_dct,
            }

        return RwInstUpdater._assign_stack_for_def_cl(stack, updated)

    @staticmethod
    def _do_commands(
        current_rw_inst: Dict[str, Any],
        rw_inst_from_option: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        keys_for_removal: List[str] = []
        for alias, value in rw_inst_from_option.items():
            if isinstance(value, str):
                current_rw_inst, keys_for_removal = RwInstUpdater._apply_command(
                    current_rw_inst, keys_for_removal, alias, value
                )
        rw_inst_from_option = {
            key: val for key, val in rw_inst_from_option.items()
            if key not in keys_for_removal
        }
        return current_rw_inst, rw_inst_from_option

    @staticmethod
    def _apply_command(
        current_rw_inst: Dict[str, Any],
        keys_for_removal: List[str],
        alias: str,
        value: str
    ) -> Tuple[Dict[str, Any], List[str]]:
        value_l = value.lower()
        if value_l in ["del", "delete", "remove", "drop"]:
            if alias in current_rw_inst:
                del current_rw_inst[alias]
            keys_for_removal.append(alias)
        elif value_l in ["clean", "new", "new_inst", "new_instance"]:
            def_inst = current_rw_inst[alias]
            if isinstance(def_inst, Values):
                current_rw_inst[alias] = Values()
            elif isinstance(def_inst, Variables):
                current_rw_inst[alias] = Variables()
            keys_for_removal.append(alias)
        else:
            current_rw_inst[value] = current_rw_inst.pop(alias)
            keys_for_removal.append(alias)

        return current_rw_inst, keys_for_removal

    @staticmethod
    def _get_classes(rw_inst: Optional[Dict[str, Any]]) -> List[Type]:
        return list(map(lambda x: type(x), rw_inst.values()))

    @staticmethod
    def _assign_stack_for_def_cl(
            stack: str, updated_cl: Dict[str, Any]) -> Dict[str, Any]:
        for alias, inst in updated_cl.items():
            if isinstance(inst, (Values, Variables)):
                inst._op_stack_name = stack
                updated_cl[alias] = inst
        return updated_cl

    @staticmethod
    def _separate_cls_inst(rw_inst: Dict[str, Any], cls: Any) -> Dict[str, Any]:
        return {alias: inst for alias, inst in
                rw_inst.items() if isinstance(inst, cls)}

    @staticmethod
    def _filter_from_def_cl(rw_inst: Dict[str, Any]) -> Dict[str, Any]:
        return {
            alias: inst for alias, inst in rw_inst.items()
            if not (isinstance(inst, Values) or isinstance(inst, Variables))
        }

    @staticmethod
    def _merge_class(
        cls: Any,
        input_rw_inst: Dict[str, Any],
        opt_rw_inst: Dict[str, Any]
    ):
        cls_input = RwInstUpdater._separate_cls_inst(input_rw_inst, cls)
        cls_opt = RwInstUpdater._separate_cls_inst(opt_rw_inst, cls)
        return cls_opt if cls_opt else cls_input

    @staticmethod
    def _merge_rw_inst(
        current_rw_inst: Dict[str, Any],
        rw_inst_from_option: Dict[str, Any]
    ) -> Dict[str, Any]:
        all_classes = [
            *RwInstUpdater._get_classes(current_rw_inst),
            *RwInstUpdater._get_classes(rw_inst_from_option)
        ]
        list_dicts = list(map(
            lambda cls: RwInstUpdater._merge_class(
                cls, current_rw_inst, rw_inst_from_option), all_classes))
        result: Dict[str, Any] = {
            k: v for d in list_dicts for k, v in d.items()}

        all_res_clss = RwInstUpdater._get_classes(result)
        if Values not in all_res_clss:
            result["val"] = Values()
        if Variables not in all_res_clss:
            result["var"] = Variables()

        return result

    @staticmethod
    def _validate_rw_inst(
            stack: str,
            current_rw_inst: Dict[str, Any],
            rw_inst_from_option: Dict[str, Any]) -> None:
        if rw_inst_from_option:
            RwInstUpdater._aliases_check(
                stack, rw_inst_from_option)
            RwInstUpdater._instances_check(
                stack, rw_inst_from_option)
            RwInstUpdater._unique_classes_check(
                stack, rw_inst_from_option)
            RwInstUpdater._non_existent_aliases_check(
                stack, current_rw_inst, rw_inst_from_option)
            RwInstUpdater._default_rw_delete_check(
                stack, current_rw_inst, rw_inst_from_option)
            RwInstUpdater._get_a_new_instance_check(
                stack, current_rw_inst, rw_inst_from_option)
            RwInstUpdater._run_conf_naming_check(
                stack, rw_inst_from_option)

    @staticmethod
    def _aliases_check(
            stack: str,
            rw_inst_from_option: Dict[str, Any]) -> None:
        all_aliases_str = all(isinstance(alias, str) for
                              alias in list(rw_inst_from_option))
        if not all_aliases_str:
            raise TypeError(
                f"Operation: {stack}. "
                f"All aliases should be string type.")

    @staticmethod
    def _instances_check(
            stack: str,
            rw_inst_from_option: Dict[str, Any]) -> None:
        all_values_are_not_classes = all(
            (type(value) != type) or not isinstance(value, str)
            for value in list(rw_inst_from_option.values()))
        if not all_values_are_not_classes:
            raise TypeError(
                f"Operation: {stack}. "
                f"All values must be initialized "
                f"instances of classes or string action")

    @staticmethod
    def _unique_classes_check(
            stack: str,
            rw_inst_from_option: Dict[str, Any]) -> None:
        rw_types = list(map(lambda x: type(x),
                            rw_inst_from_option.values()))
        rw_types = [t for t in rw_types if t != str]
        if len(rw_types) != len(set(rw_types)):
            raise TypeError(
                f"Operation: {stack}. "
                f"All special classes must be unique.")

    @staticmethod
    def _non_existent_aliases_check(
            stack: str,
            current_rw_inst: Dict[str, Any],
            rw_inst_from_option: Dict[str, Any]) -> None:
        non_existent_aliases = []
        for alias, value in rw_inst_from_option.items():
            if isinstance(value, str) and alias not in current_rw_inst:
                non_existent_aliases.append(alias)
        if non_existent_aliases:
            raise TypeError(
                f"Operation: {stack}. "
                f"Aliases to which commands were to be "
                f"applied were not found in base rw_inst.\n"
                f"Non-existent aliases: {non_existent_aliases}")

    @staticmethod
    def _default_rw_delete_check(
            stack: str,
            current_rw_inst: Dict[str, Any],
            rw_inst_from_option: Dict[str, Any]) -> None:
        for alias, value in rw_inst_from_option.items():
            if isinstance(value, str) and value.lower() in [
                "del", "delete", "remove", "drop"] and (
                    isinstance(current_rw_inst[alias], Values) or
                    isinstance(current_rw_inst[alias], Variables) or
                    isinstance(current_rw_inst[alias], RunConfigurations)):
                raise TypeError(
                    f"Operation: {stack}. "
                    f"You cannot delete instances of "
                    f"default classes Values(), Variables(),\n"
                    f"but they can be completely replaced "
                    f"with a new instance by the "
                    f"'clean', 'new', 'new_inst', "
                    f"'new_instance' commands.\n"
                    f"Also, you cannot delete or 'clean' "
                    f"by the command default RunConfiguratons()")

    @staticmethod
    def _get_a_new_instance_check(
            stack: str,
            current_rw_inst: Dict[str, Any],
            rw_inst_from_option: Dict[str, Any]) -> None:
        for alias, value in rw_inst_from_option.items():
            if isinstance(value, str) and value.lower() in [
                "clean", "new", "new_inst", "new_instance"] and (
                    not isinstance(
                        current_rw_inst[alias], Values) and not isinstance(
                current_rw_inst[alias], Variables)):
                raise TypeError(
                    f"Operation: {stack}. "
                    f"You can get a new instance by executing command: "
                    f"'clean', 'new', 'new_inst', 'new_instance'\n"
                    f"only for default rw instance Values(), Variables().")

    @staticmethod
    def _run_conf_naming_check(
            stack: str,
            rw_inst_from_option: Dict[str, Any]) -> None:
        for alias, value in rw_inst_from_option.items():
            if (isinstance(value, RunConfigurations) and
                not alias == "run_conf") or (
                    alias == "run_conf" and isinstance(value, str)):
                raise TypeError(
                    f"Operation: {stack}. "
                    f"The default rw instance RunConfiguratons() "
                    f"can only have 'run_conf' alias")


class BranchOptInterface(ABC):
    def get_new_instance(self) -> "BranchOptInterface":
        pass


@dataclass
class BranchOptions(BranchOptInterface):
    br_name: Optional[str] = None
    processor: Optional[Type["Processor"]] = None
    end_chain_cond: Optional[Callable] = None
    raise_err_cond: Optional[Callable] = None
    assign: Optional[Tuple[str, ...]] = None
    hide_log_inf: Tuple[Optional[bool], Optional[bool]] = (None, None)
    check_type_strategy_all: Optional[bool] = None
    rw_inst: Union[Dict[str, Any], Tuple[Dict[str, Any], ...]] = ()
    distribute_input_data: bool = False
    force_call: bool = False
    delayed_return: Optional[Tuple] = None

    def get_new_instance(self) -> "BranchOptions":
        new_inst = BranchOptions()
        for field, value in self.__dict__.items():
            setattr(new_inst, field, value)
        return new_inst


@dataclass
class RunConfigurations:
    """Store the latest run configurations."""
    opt_stack: Optional[Tuple[BranchOptions, ...]] = None
    br_opt: Optional[BranchOptions] = None

    stack_divider: str = " -> "
    last_op_stack: str = INITIAL_RUN
    operation_stack: Optional[str] = INITIAL_RUN

    def set_default_processor(self, processor: Type["Processor"]) -> None:
        self.opt_stack[0].processor = processor

    def add_br_opt_to_stack(self, br_opt: BranchOptions) -> None:
        stack = f"{self.get_branch_stack()} (branch)"
        merged = RwInstUpdater.get_updated_all(
            stack, self.get_rw_inst(), br_opt.rw_inst)
        renewed = RunConfigurations._renew_def_rw_inst(stack, merged)

        self.opt_stack += (br_opt,)
        self.br_opt = br_opt
        self.br_opt.rw_inst = renewed

        self._update_opt_stack()

    def get_branch_stack(self) -> str:
        if len(self.opt_stack) == 1:
            return INITIAL
        names = [opt.br_name for opt in self.opt_stack][1:]
        return self.stack_divider.join(names)

    def set_operation_stack(self, op_name: str) -> None:
        self.operation_stack = (f"{self.get_branch_stack()}"
                                f"{self.stack_divider}{op_name}")

    def update_last_rw_inst(self, rw_inst: Tuple[Dict[str, Any], ...]) -> None:
        base = self.get_rw_inst()
        merged = RwInstUpdater.get_updated_all(
            self.operation_stack, base, rw_inst)
        self.br_opt.rw_inst = merged

    def get_rw_inst(self) -> Dict[str, Any]:
        base_map = dict(self.br_opt.rw_inst) if (
                self.br_opt and self.br_opt.rw_inst) else {}
        return {**base_map, "run_conf": self}

    def get_renewed_self_instance(self) -> 'RunConfigurations':
        new_rw_inst = RunConfigurations._renew_def_rw_inst(
            self.operation_stack, self.get_rw_inst())
        return new_rw_inst["run_conf"]

    def pop_stack(self):
        new_delay_return = self._pop_delayed_return()
        new_rw_inst_items = self._pop_rw_inst()
        new_map = dict(new_rw_inst_items)

        self.opt_stack = self.opt_stack[:-1]
        self.br_opt = self.opt_stack[-1]
        if new_delay_return:
            self.br_opt.delayed_return = new_delay_return
        self.br_opt.rw_inst = new_map

    def _pop_delayed_return(self) -> Optional[Tuple]:
        last_delay_return = self.br_opt.delayed_return
        if last_delay_return is not None:
            prev_delayed_return = self.opt_stack[-2].delayed_return
            if prev_delayed_return is not None:
                return *prev_delayed_return, *last_delay_return

    def _pop_rw_inst(self) -> Tuple:
        penult_map = dict(self.opt_stack[-2].rw_inst)
        last_map = self.get_rw_inst()
        last_instances = list(last_map.values())

        prev_rw_inst: Dict[str, Any] = {}
        for alias, inst in penult_map.items():
            for obj in last_instances:
                if isinstance(obj, type(inst)):
                    prev_rw_inst[alias] = obj
        return tuple(prev_rw_inst.items())

    def _update_opt_stack(self) -> None:
        self._set_br_name()

        prev_opt = self.opt_stack[-2]

        if self.br_opt.processor is None:
            self.br_opt.processor = prev_opt.processor

        opt_init_inf, opt_all_inf = self.br_opt.hide_log_inf
        prev_init_inf, prev_all_inf = prev_opt.hide_log_inf
        self.br_opt.hide_log_inf = (
            prev_init_inf if opt_init_inf is None else opt_init_inf,
            prev_all_inf if opt_all_inf is None else opt_all_inf,
        )

        if self.br_opt.check_type_strategy_all is None:
            self.br_opt.check_type_strategy_all = prev_opt.check_type_strategy_all

        if prev_opt.distribute_input_data and not self.br_opt.distribute_input_data:
            self.br_opt.distribute_input_data = True

        self.br_opt.delayed_return = None if prev_opt.delayed_return is None else ()

    def _set_br_name(self) -> None:
        OptionsChecker.check_name(self.br_opt.br_name, self.last_op_stack)
        if self.br_opt.br_name is None:
            self.br_opt.br_name = "BRANCH NAME NOT DEFINED"

    @staticmethod
    def _renew_def_instance(
            stack: str,
            old_rw_inst: Dict[str, Any],
            rw_class: Type) -> Dict[str, Any]:
        for alias, rw_inst in old_rw_inst.items():
            if isinstance(rw_inst, rw_class):
                new_inst = rw_class()
                new_inst._op_stack_name = stack
                for field, value in rw_inst.__dict__.items():
                    setattr(new_inst, field, value)
                return {alias: new_inst}

    @staticmethod
    def _renew_run_conf(old_rw_inst: Dict[str, Any]) -> Dict[str, Any]:
        for alias, rw_inst in old_rw_inst.items():
            if isinstance(rw_inst, RunConfigurations):
                new_inst = RunConfigurations()
                for field, value in rw_inst.__dict__.items():
                    setattr(new_inst, field, value)
                new_opt_inst = new_inst.br_opt.get_new_instance()
                new_inst.br_opt = new_opt_inst
                return {alias: new_inst}

    @staticmethod
    def _renew_def_rw_inst(stack: str, rw_inst: Dict[str, Any]) -> Dict[str, Any]:
        if rw_inst:
            return {
                **rw_inst,
                **RunConfigurations._renew_def_instance(stack, rw_inst, Values),
                **RunConfigurations._renew_def_instance(stack, rw_inst, Variables),
                **RunConfigurations._renew_run_conf(rw_inst),
            }
        return rw_inst

    def __post_init__(self):
        br_opt = BranchOptions()
        br_opt.br_name = DEFAULT_BRANCH_OPTIONS
        br_opt.hide_log_inf = (False, False)
        br_opt.check_type_strategy_all = True
        br_opt.distribute_input_data = False
        root_map = {"val": Values(), "var": Variables(), "run_conf": self}
        br_opt.rw_inst = root_map
        br_opt.delayed_return = None

        self.opt_stack = (br_opt,)
        self.br_opt = br_opt

    def __repr__(self):
        return f"{self.__class__.__name__}()"
