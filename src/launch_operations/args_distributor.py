from typing import Any, Optional, Tuple, Dict

from src.dataclasses import LaunchParameters
from src.launch_operations.errors import DistributionError, AssignmentError
from src.launch_operations.launch_utils import to_tuple


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
                              par: LaunchParameters) -> LaunchParameters:
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
                       par: LaunchParameters) -> None:
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
                     fields_for_assign: Tuple[str, ...],
                     last_obj_name: str) -> None:
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
                 par: LaunchParameters) -> Tuple[Optional[Any], Optional[Tuple], LaunchParameters]:
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
            par: LaunchParameters) -> Tuple[Optional[Any], Optional[Tuple], Optional[Tuple], LaunchParameters]:
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

        new_par = LaunchParameters()
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
            par: LaunchParameters) -> Tuple[Optional[Any], Optional[Tuple], Optional[Tuple], LaunchParameters]:
        # ArgsDistributor.rem_args_check(
        #     rem_args, last_obj_name, distribute_args, par)

        # result, rem_args, par = ArgsDistributor.do_delay(
        #     result, data_before_call, rem_args, par)

        distribute_args, result, rem_args, par = ArgsDistributor.prepare_output_data(
            init_data, distribute_args, result, rw_inst, last_op_name, rem_args, par)

        return result, rem_args, distribute_args, par
