from typing import Any, Dict, Optional, List, Type

from src.default.rw_classes import Values, Variables


class RwInstUpdater:
    @staticmethod
    def get_updated(
            stack: str,
            current_rw_inst: Optional[Dict[str, Any]],
            rw_inst_from_option: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        base_opt = rw_inst_from_option
        current_rw_inst = {} if current_rw_inst is None else current_rw_inst
        rw_inst_from_option = {} if rw_inst_from_option is None else rw_inst_from_option
        RwInstUpdater._validate_rw_inst(stack, rw_inst_from_option)

        updated_cl = RwInstUpdater._merge_rw_inst(current_rw_inst, rw_inst_from_option)
        if base_opt == {}:
            return {**RwInstUpdater._separate_cls_inst(updated_cl, Values),
                    **RwInstUpdater._separate_cls_inst(updated_cl, Variables)}
        return updated_cl

    @staticmethod
    def _get_classes(rw_inst: Optional[Dict[str, Any]]) -> List[Type]:
        return list(map(lambda x: type(x), rw_inst.values()))

    @staticmethod
    def _separate_cls_inst(rw_inst: Optional[Dict[str, Any]],
                           cls: Any) -> Dict[str, Any]:
        return {alias: inst for alias, inst in rw_inst.items() if isinstance(inst, cls)}

    @staticmethod
    def _filter_from_def_cl(rw_inst: Dict[str, Any]) -> Dict[str, Any]:
        return {alias: inst for alias, inst in rw_inst.items()
                if not (isinstance(inst, Values) or isinstance(inst, Variables))}

    @staticmethod
    def _merge_class(cls: Any, input_rw_inst: Dict[str, Any], opt_rw_inst: Dict[str, Any]):
        cls_input = RwInstUpdater._separate_cls_inst(input_rw_inst, cls)
        cls_opt = RwInstUpdater._separate_cls_inst(opt_rw_inst, cls)
        return cls_opt if cls_opt else cls_input

    @staticmethod
    def _merge_rw_inst(current_rw_inst: Dict[str, Any],
                       rw_inst_from_option: Dict[str, Any]) -> Dict[str, Any]:
        all_classes = [*RwInstUpdater._get_classes(current_rw_inst),
                       *RwInstUpdater._get_classes(rw_inst_from_option)]

        list_dicts = list(map(lambda cls: RwInstUpdater._merge_class(
            cls, current_rw_inst, rw_inst_from_option), all_classes))

        result = {k: v for d in list_dicts for k, v in d.items()}
        all_res_clss = RwInstUpdater._get_classes(result)
        if Values not in all_res_clss:
            result["val"] = Values()
        if Variables not in all_res_clss:
            result["var"] = Variables()

        return result

    @staticmethod
    def _validate_rw_inst(stack: str, rw_inst: Dict[str, Any]) -> None:
        if rw_inst:
            all_aliases_str = all(isinstance(alias, str) for alias in list(rw_inst))
            if not all_aliases_str:
                raise TypeError(f"Operation: {stack}. "
                                f"All aliases should be string type.")

            all_values_are_not_classes = all(type(value) != type for value in list(rw_inst.values()))
            if not all_values_are_not_classes:
                raise TypeError(f"Operation: {stack}. "
                                f"All values must be initialized instances of classes.")

            rw_types = list(map(lambda x: type(x), rw_inst.values()))
            if len(rw_types) != len(set(rw_types)):
                raise TypeError(f"Operation: {stack}. "
                                f"All special classes must be unique.")
