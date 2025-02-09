from typing import Any, Dict, List, Optional, Tuple, Union, NoReturn, Type

from src.default.rw_classes import Values, Variables
from src.dataclasses import LParameters, Flags
from src.launch_operations.result_parser import ResultParser
from src.processor.one_op_processor import process_one_operation
from src.utils.formatters import LoggerBuilder


log = LoggerBuilder().build()


class EmptyDataError(Exception):
    pass


class DistributionError(Exception):
    pass


class AssignmentError(Exception):
    pass


class ArgumentError(Exception):
    pass


def to_tuple(data: Any) -> Tuple:
    return (data,) if not isinstance(data, Tuple) else data


class ArgsDistributor:
    # @staticmethod
    # def fork_postprocessing(rem_args: Optional[Any], init_data: Tuple, par: Parameters
    #                         ) -> Tuple[Optional[Any], Tuple, Parameters]:
    #     if par.many_operations and par.flags.distribute_result:
    #         rem_args = None
    #     if par.many_operations and not par.flags.distribute_result and par.delayed_pos_args is not None:
    #         init_data = ()
    #         par.flags.is_it_delay_return = False
    #     return rem_args, init_data, par

    @staticmethod
    def _do_stop_distribution(distribute_args: Optional[Any],
                              par: LParameters) -> LParameters:
        if distribute_args is not None and par.flags.distribute_result or not len(
                par.remaining_operations) and distribute_args is not None:
            par.stop_distribution = True
        return par

    @staticmethod
    def get_data_for_call(
            is_it_delay_return: bool,
            data_before_call: Optional[Any],
            distribute_args: Optional[Any]) -> Tuple[Tuple, Optional[Any]]:
        data_for_call = data_before_call
        if is_it_delay_return:
            data_for_call = distribute_args
        return data_for_call, data_before_call

    @staticmethod
    def get_init_data(
            init_data: Optional[Any],
            distribute_args: Optional[Any]) -> Optional[Any]:
        if init_data is None and distribute_args is None:
            return None, None
        elif init_data is None and distribute_args is not None:
            return to_tuple(distribute_args), ()
        elif init_data is not None and distribute_args is None:
            return to_tuple(init_data), None
        return to_tuple(distribute_args), to_tuple(init_data)

        # if distribute_args is not None:

    #
    # @staticmethod
    # def get_init_data(
    #         par: Parameters,
    #         init_data: Optional[Any],
    #         distribute_args: Optional[Any]) -> Tuple[Optional[Any], Parameters]:
    #     # par = ArgsDistributor._do_stop_distribution(distribute_args, par)
    #     init_data, par = ArgsDistributor.get_data(init_data, distribute_args, par)
    #     # if par.flags.distribute_result and not par.flags.stop_distribution:
    #     #     par.flags.is_it_delay_return = False
    #     return init_data, par
    #
    # @staticmethod
    # def get_data(init_data: Optional[Any],
    #              distribute_args: Optional[Any],
    #              par: Parameters) -> Optional[Any]:
    #     if distribute_args is not None:
    #         # if par.delayed_pos_args is None:
    #         #     par.delayed_pos_args = ()
    #         if init_data is None:
    #             return ()
    #         return to_tuple(init_data)
    #     return init_data

    @staticmethod
    def rem_args_check(remaining_args: Optional[Tuple],
                       last_op_name: str,
                       distribute_args: Optional[Tuple],
                       par: LParameters) -> NoReturn:
        if remaining_args is not None and not par.flags.burn_rem_args:
            text_err = f"Stack: {last_op_name}. " \
                       f"\nArguments were found that were not involved in either " \
                       f"the call or the initialization of the class. " \
                       f"Number of positions: {len(remaining_args)}, " \
                       f"types: {tuple([type(arg) for arg in remaining_args])}. "
            text_suggestion = f"You can use the distribution_args parameter and set the " \
                              f"Add.stop_distribution option where you want the data returned. " \
                              f"Or you can use the add.burn_rem_args option to reset them."
            if par.flags.stop_distribution:
                raise DistributionError(f"{text_err}After distribution stops, "
                                        f"remaining arguments are invalid. {text_suggestion}")
            elif distribute_args is not None and par.flags.distribute_result:
                raise DistributionError(f"{text_err}When starting the re-distribution of arguments, "
                                        f"there should be no remaining arguments. {text_suggestion}")
            elif par.delayed_return is None and distribute_args is None:
                raise DistributionError(
                    f"{text_err}Their use in the next operation is not provided. {text_suggestion}")

    @staticmethod
    def assign_check(result: Optional[Any],
                     fields_for_assign: Tuple[str],
                     last_obj_name: str) -> NoReturn:
        if result is None:
            raise AssignmentError(f"Stack: {last_obj_name}. The result of the operation is None. "
                                  f"Assignment is not possible.")
        len_result = len(to_tuple(result))
        if len_result != len(fields_for_assign):
            raise ValueError(f"Stack: {last_obj_name}. The number of positional arguments after "
                             f"the operation execution is {len_result} and it is not equal to "
                             f"the number of fields to assign, they were found {len(fields_for_assign)}")

    @staticmethod
    def do_delay(result: Optional[Any],
                 data_before_call: Tuple,
                 rem_args: Optional[Tuple],
                 par: LParameters) -> Tuple[Optional[Any], Optional[Tuple], LParameters]:
        print("UUUUUUUUUUUU", data_before_call, rem_args, result)
        if par.flags.distribute_result and not par.flags.is_it_delay_return:
            par.flags.is_it_delay_return = True
            return (), to_tuple(result), par
        elif par.flags.distribute_result and par.flags.is_it_delay_return and par.flags.stop_distribution:
            return (), (*data_before_call, *to_tuple(result)), par
        elif par.flags.is_it_delay_return or par.flags.stop_distribution:# and not par.flags.stop_distribution:
            return (*data_before_call, *to_tuple(result)), rem_args, par
        # elif par.flags.stop_distribution:
        #     # par.flags.burn_rem_args = True
        #     return (*data_before_call, *to_tuple(result)), rem_args, par
        return result, rem_args, par

    @staticmethod
    def prepare_output_data(
            init_data: Tuple,
            distribute_args: Optional[Tuple],
            result: Optional[Any],
            rw_inst: Dict[str, Any],
            last_op_name: str,
            rem_args: Optional[Tuple],
            par: LParameters) -> Tuple[Optional[Any], Optional[Tuple], Optional[Tuple], LParameters]:
        print("HHHHHHHHHin", f"id:{init_data}", f"da:{distribute_args}", f"res:{result}", f"dr:{par.delayed_return}", last_op_name, rem_args)
        print("HHHHHHHHHin", par.flags)
        if not len(par.remaining_operations):
            par.flags.stop_distribution = True
        ArgsDistributor.rem_args_check(rem_args, last_op_name, distribute_args, par)
        if par.flags.burn_rem_args:
            rem_args = None
        if distribute_args is not None:
            if par.flags.stop_distribution and not par.flags.distribute_result:
                if par.delayed_return is not None:
                    result = (*par.delayed_return, *to_tuple(result))
                    distribute_args = None
                    par.delayed_return = None
            elif par.flags.stop_distribution and par.flags.distribute_result:
                if par.delayed_return is not None:
                    distribute_args = (*par.delayed_return, *to_tuple(result))
                    result = ()
                    par.delayed_return = ()
            elif not par.flags.stop_distribution and par.flags.distribute_result:
                if par.delayed_return is None:
                    distribute_args = to_tuple(result)
                    result = ()
                    par.delayed_return = ()
                elif par.delayed_return is not None:
                    distribute_args = (*par.delayed_return, *to_tuple(result))
                    result = ()
                    par.delayed_return = ()
            elif not par.flags.stop_distribution and not par.flags.distribute_result:
                if par.delayed_return is None:
                    par.delayed_return = to_tuple(result)
                    result = ()
                    distribute_args = rem_args if rem_args is not None else ()
                elif par.delayed_return is not None:
                    if par.many_operations:
                        par.delayed_return = None
                        distribute_args = None
                    else:
                        par.delayed_return = (*par.delayed_return, *to_tuple(result))
                        result = ()
                        distribute_args = rem_args if rem_args is not None else ()
        elif distribute_args is None:
            if not par.flags.stop_distribution and par.flags.distribute_result:
                if par.delayed_return is None:
                    distribute_args = to_tuple(result)
                    result = ()

        print("MANY:", par.many_operations)
        if par.many_operations and not par.flags.distribute_result and distribute_args is not None:
            result = distribute_args
            distribute_args = None
            par.delayed_return = None

        print("HHHHHHHHHout", f"id:{init_data}", f"da:{distribute_args}", f"res:{result}", f"dr:{par.delayed_return}", last_op_name, rem_args)
        print("HHHHHHHHHout", par.flags)

        if par.flags.stop_distribution and not par.flags.distribute_result:
            result = result[0] if isinstance(result, Tuple) and len(result) == 1 else result
            distribute_args = None

        print(22, distribute_args, rem_args, par)

        if par.fields_for_assign is not None:
            ArgsDistributor.assign_check(result, par.fields_for_assign, last_op_name)
            format_rw_inst = {el: rw_inst[el.split(".")[0]] for el in par.fields_for_assign}
            result = assign(*to_tuple(result), **format_rw_inst)

        new_par = LParameters()
        new_par.operations = par.remaining_operations
        new_par.flags.all_operations_must_be_executed = par.flags.all_operations_must_be_executed
        new_par.delayed_return = par.delayed_return
        new_par.last_op_name = last_op_name

        print(44, distribute_args, result, rem_args, new_par.delayed_return, new_par)
        return distribute_args, result, rem_args, new_par

    @staticmethod
    def get_args_for_next_operation(
            result: Optional[Any],
            init_data: Tuple,
            rem_args: Optional[Tuple],
            distribute_args: Optional[Any],
            last_op_name: str,
            rw_inst: Dict[str, Any],
            par: LParameters) -> Tuple[Optional[Any], Optional[Tuple], Optional[Tuple], LParameters]:
        # ArgsDistributor.rem_args_check(
        #     rem_args, last_obj_name, distribute_args, par)

        # result, rem_args, par = ArgsDistributor.do_delay(
        #     result, data_before_call, rem_args, par)

        distribute_args, result, rem_args, par = ArgsDistributor.prepare_output_data(
            init_data, distribute_args, result, rw_inst, last_op_name, rem_args, par)

        return result, rem_args, distribute_args, par


def get_stack_name(name_stack: str, op_name: Optional[str]) -> str:
    if op_name is None:
        return name_stack
    return f"{name_stack} -> {op_name}"


class RwInstUpdater:
    @staticmethod
    def get(stack_name: str,
            opt_rw_inst: Optional[Dict[str, Any]],
            input_rw_inst: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        base_opt = opt_rw_inst
        opt_rw_inst = {} if opt_rw_inst is None else opt_rw_inst
        input_rw_inst = {} if input_rw_inst is None else input_rw_inst
        RwInstUpdater._validate_rw_inst(stack_name, opt_rw_inst)
        RwInstUpdater._validate_rw_inst(stack_name, input_rw_inst)

        updated_cl = RwInstUpdater._merge_rw_inst(opt_rw_inst, input_rw_inst)
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
    def _merge_class(cls: Any, opt_rw_inst: Dict[str, Any], input_rw_inst: Dict[str, Any]):
        cls_input = RwInstUpdater._separate_cls_inst(input_rw_inst, cls)
        cls_opt = RwInstUpdater._separate_cls_inst(opt_rw_inst, cls)
        return cls_opt if cls_opt else cls_input

    @staticmethod
    def _merge_rw_inst(opt_rw_inst: Dict[str, Any], input_rw_inst: Dict[str, Any]) -> Dict[str, Any]:
        all_classes = [*RwInstUpdater._get_classes(input_rw_inst),
                       *RwInstUpdater._get_classes(opt_rw_inst)]
        list_dicts = list(map(lambda cls: RwInstUpdater._merge_class(
            cls, opt_rw_inst, input_rw_inst), all_classes))
        result = {k: v for d in list_dicts for k, v in d.items()}
        all_res_clss = RwInstUpdater._get_classes(result)
        if Values not in all_res_clss:
            result["val"] = Values()
        if Variables not in all_res_clss:
            result["var"] = Variables()

        return result

    @staticmethod
    def _validate_rw_inst(stack_name: str, rw_inst: Optional[Dict[str, Any]]) -> NoReturn:
        if rw_inst is not None:
            all_aliases_str = all(isinstance(alias, str) for alias in list(rw_inst))
            if not all_aliases_str:
                raise TypeError(f"Object_stack: {stack_name}. "
                                f"All aliases should be string type.")

            all_values_are_not_classes = all(type(value) != type for value in list(rw_inst.values()))
            if not all_values_are_not_classes:
                raise TypeError(f"Object_stack: {stack_name}. "
                                f"All values must be initialized instances of classes.")

            rw_types = list(map(lambda x: type(x), rw_inst.values()))
            if len(rw_types) != len(set(rw_types)):
                raise TypeError(f"Object_stack: {stack_name}. "
                                f"All special classes must be unique.")


class OptionsApplier:
    def __init__(self, operations,
                 rw_inst: Optional[Dict[str, Any]],
                 def_args: Optional[Tuple]):
        self._operations = operations if isinstance(operations, List) else [operations]
        self._first_operation = self._operations[0]
        self._remaining_operations = self._operations[1:]
        self._op_name = None
        self._def_args = def_args
        self._rw_inst = rw_inst
        self._one_operation = None
        self._many_operations = None
        self._fields_for_assign = None
        self._distribute_result_flag = False
        self._stop_distribution = False
        self._burn_rem_args = False
        self._raise_err_if_empty_data = False

    def _reassign_op_name(self):
        if self._first_operation._op_name is not None:
            self._op_name = self._first_operation._op_name

    def _reassign_def_args(self):
        if self._first_operation._def_args is not None:
            self._def_args = self._first_operation._def_args

    def _get_assign_tuple(self):
        if self._first_operation._assign is not None:
            self._fields_for_assign = self._first_operation._assign

    def _reassign_rw_inst(self, stack_name: str):
        new_rw_inst = self._first_operation._rw_inst if isinstance(self._first_operation, add) else None
        self._rw_inst = RwInstUpdater.get(
            stack_name, new_rw_inst, self._rw_inst)

    def _get_objects(self):
        if self._first_operation._one_operation is not None:
            self._one_operation = self._first_operation._one_operation
        if self._first_operation._many_operations is not None:
            self._many_operations = self._first_operation._many_operations

    def apply_options(self, par: LParameters) -> Tuple[
            LParameters, Optional[str], Optional[Tuple], Dict[str, Any]]:
        if isinstance(self._first_operation, add):
            self._reassign_op_name()
            OptionsChecker.check_name(self._op_name, par.last_op_name)
            self._reassign_def_args()
            self._get_objects()
            self._get_assign_tuple()
            par.flags.stop_distribution = self._first_operation._stop_distribution
            par.flags.distribute_result = self._first_operation._distribute_result
            par.flags.burn_rem_args = self._first_operation._burn_rem_args
            par.flags.raise_err_if_empty_data = self._first_operation._raise_err_if_empty_data

        stack_name = get_stack_name(par.name_stack, self._op_name)
        OptionsChecker.check_burn_rem_args(par.flags, stack_name)
        self._reassign_rw_inst(stack_name)
        par.one_operation = self._one_operation if self._one_operation else self._first_operation
        par.many_operations = self._many_operations
        par.remaining_operations = self._remaining_operations
        par.fields_for_assign = self._fields_for_assign
        OptionsChecker.check_assign_option(par.fields_for_assign, self._rw_inst, stack_name)

        return par, self._op_name, self._def_args, self._rw_inst


class OptionsChecker:
    @staticmethod
    def check_name(name: Optional[str], last_op_name: str) -> NoReturn:
        if name is not None and not isinstance(name, str):
            raise TypeError(f"The last successful operation: {last_op_name}. "
                            f"The name of the operation passed in the option "
                            f"must be in string format.")

    @staticmethod
    def check_assign_option(fields_for_assign: Optional[Tuple[str, ...]],
                            rw_inst: Dict[str, Any],
                            stack_name: str) -> NoReturn:
        if fields_for_assign is not None:
            if not all(map(lambda x: isinstance(x, str), fields_for_assign)):
                raise TypeError(f"Stack: {stack_name}. All values to assign must be string only.")
            aliases = list(map(lambda x: x.split(".")[0], fields_for_assign))
            for alias in aliases:
                if alias not in rw_inst:
                    raise AssignmentError(f"Stack: {stack_name}. Alias \"{alias}\" "
                                          f"is missing from rw_inst. Assignment not possible.")

    @staticmethod
    def check_burn_rem_args(flags: Flags, stack_name: str) -> NoReturn:
        if flags.burn_rem_args and flags.distribute_result:
            raise ArgumentError(f"Stack: {stack_name}. It is not possible to simultaneously "
                                f"burn the remaining arguments and distribute the result.")


def create_operation_for_fork(
        operations: List[Any],
        op_name: Optional[str],
        init_data: Tuple,
        rw_inst: Dict[str, Any],
        def_args: Optional[Tuple],
        distribute_args: Optional[Tuple],
        all_operations_must_be_executed: bool) -> Tuple[Dict, Tuple]:
    return {run: {
        "operations": operations,
        "init_data": init_data,
        "op_name": op_name,
        "rw_inst": rw_inst,
        "def_args": def_args,
        "distribute_args": distribute_args,
        "all_operations_must_be_executed": all_operations_must_be_executed}}


class DataParser:
    @staticmethod
    def parse_data(init_data: Optional[Any],
                   distribute_args: Optional[Any],
                   def_args: Optional[Tuple],
                   rw_inst: Dict[str, Any],
                   par: LParameters) -> Optional[Tuple[Tuple, Optional[Tuple], Dict[str, Any], LParameters]]:
        print("parse_data11111111111", f"id:{init_data}", f"da:{distribute_args}", f"dr:{par.delayed_return}", f"def:{def_args}", rw_inst)
        print("parse_data11111111111", par.flags)
        da_sd, rw_inst = ResultParser.sort_data(to_tuple(distribute_args), rw_inst)
        if da_sd.stop_all_further_operations_with_success:
            return None
        id_sd, rw_inst = ResultParser.sort_data(to_tuple(init_data), rw_inst)
        if id_sd.stop_all_further_operations_with_success:
            return None

        if init_data is None and distribute_args is None and \
                def_args is None and not par.flags.distribute_result:
            return None
        elif init_data is None and distribute_args is None and \
                def_args is not None and not par.flags.distribute_result:
            init_data = def_args
        elif init_data is None and distribute_args is None and par.flags.distribute_result:
            init_data = ()
        elif init_data is None and distribute_args is not None:
            init_data, distribute_args = da_sd.data, ()
        elif init_data is not None and distribute_args is None:
            init_data = id_sd.data
        else:
            par.delayed_return = id_sd.data if par.delayed_return is None else (
                *par.delayed_return, *id_sd.data)
            if not par.many_operations:
                init_data = da_sd.data
                distribute_args = ()
            else:
                init_data = ()
                distribute_args = da_sd.data

        print("parse_data22222222222", f"id:{init_data}", f"da:{distribute_args}", f"dr:{par.delayed_return}", f"def:{def_args}", rw_inst)
        print("parse_data22222222222", par.flags)
        return init_data, distribute_args, rw_inst, par

    @staticmethod
    def check_result(result: Optional[Tuple], op_name: Optional[str], par: LParameters) -> None:
        if result is None:
            stack_name = get_stack_name(par.name_stack, op_name)
            if par.flags.all_operations_must_be_executed:
                raise EmptyDataError(
                    f"Stack: {stack_name}. "
                    f"The data was not received when all operations were "
                    f"scheduled to be performed.")
            elif par.flags.raise_err_if_empty_data:
                raise EmptyDataError(
                    f"Stack: {stack_name}. "
                    f"No data was received. An exception was raised, "
                    f"according to the add.raise_err_if_empty_data flag set.")

    @staticmethod
    def get_result(init_data: Optional[Any],
                   distribute_args: Optional[Any],
                   def_args: Optional[Tuple],
                   rw_inst: Dict[str, Any],
                   op_name: Optional[str],
                   par: LParameters) -> Optional[Tuple[Tuple, Optional[Tuple], Dict[str, Any], LParameters]]:
        result = DataParser.parse_data(init_data, distribute_args, def_args, rw_inst, par)
        DataParser.check_result(result, op_name, par)
        return result


def get_default_op_name_stack(op_name: Optional[str]) -> Tuple[str, None]:
    name_stack = op_name
    if name_stack is None:
        return "BRANCH NAME NOT DEFINED", None
    return name_stack, None


def get_parameters(
        operations: Union[Any, List[Any], LParameters],
        name_stack: Optional[str]) -> LParameters:
    if isinstance(operations, LParameters):
        operations.name_stack = name_stack
        return operations
    return LParameters(operations=operations, name_stack=name_stack)


def create_fork_if_requred(
        par: LParameters,
        op_name: Optional[str],
        init_data: Tuple,
        rw_inst: Dict[str, Any],
        def_args: Optional[Tuple],
        distribute_args: Optional[Tuple]) -> Tuple[Tuple, LParameters]:
    if par.many_operations:
        op_name = f"{par.name_stack} -> {op_name}" if op_name else \
            f"{par.name_stack} -> BRANCH NAME NOT DEFINED"
        par.one_operation = create_operation_for_fork(
            par.many_operations, op_name, init_data, rw_inst,
            def_args, distribute_args, par.flags.all_operations_must_be_executed)
        return (), par
    return init_data, par


def get_default_args(
        operations: Union[Any, List[Any]],
        init_data: Optional[Any],
        def_args: Optional[Tuple],
        distribute_args: Optional[Any]) -> Optional[Tuple]:
    return () if not isinstance(operations, LParameters) and \
                 init_data is None and \
                 def_args is None and \
                 distribute_args is None else def_args


def run(operations: Union[Any, List[Any]],
        init_data: Optional[Any] = None,
        op_name: Optional[str] = None,
        rw_inst: Optional[Dict[str, Any]] = None,
        def_args: Optional[Tuple] = None,
        distribute_args: Optional[Any] = None,
        all_operations_must_be_executed: bool = False) -> Any:

    print()
    print(11111111111111, op_name, init_data, rw_inst, def_args, distribute_args, operations)

    def_args = get_default_args(
        operations, init_data, def_args, distribute_args)
    name_stack, op_name = get_default_op_name_stack(op_name)
    par = get_parameters(operations, name_stack)

    par, op_name, def_args, rw_inst = OptionsApplier(
            par.operations, rw_inst, def_args).apply_options(par)

    # init_data, distribute_args = ArgsDistributor.get_init_data(init_data, distribute_args)

    print(22222222222222, init_data, par.name_stack, op_name, init_data, distribute_args, rw_inst, def_args)

    res = DataParser.get_result(
        init_data, distribute_args, def_args, rw_inst, op_name, par)
    if res is None:
        return None
    init_data, distribute_args, rw_inst, par = res

    print(20002222222200022222, init_data, distribute_args, rw_inst)
    init_data, par = create_fork_if_requred(
        par, op_name, init_data, rw_inst, def_args, distribute_args)

    # data_for_call, init_data = ArgsDistributor.get_data_for_call(
    #     par.flags.is_it_delay_return, init_data, distribute_args)

    print(333333333333333, init_data, par.name_stack, op_name, rw_inst, distribute_args)
    result, last_op_name, rem_args = process_one_operation(
        par.one_operation, init_data, rw_inst, name_stack=par.name_stack, op_name=op_name)


    print(444444444444444, result, init_data, last_op_name, rem_args, distribute_args)
    # rem_args, init_data, par = ArgsDistributor.fork_postprocessing(
    #     rem_args, init_data, par)

    result, rem_args, distribute_args, par = ArgsDistributor.get_args_for_next_operation(
        result, init_data, rem_args, distribute_args, last_op_name, rw_inst, par)

    if not len(par.operations):
        return result

    print(666666666666666, par)
    print(666666666666667, par.flags)
    print(666666666666668, result, last_op_name, rw_inst, rem_args, par.remaining_operations, def_args, distribute_args)
    return run(par,
               init_data=result,
               op_name=par.name_stack,
               rw_inst=rw_inst,
               distribute_args=distribute_args,
               all_operations_must_be_executed=all_operations_must_be_executed)
