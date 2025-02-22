from typing import Any, Dict, List, Optional, Tuple, Union

from src.dataclasses import LaunchParameters, Configurations
from src.launch_operations.args_distributor import ArgsDistributor
# from src.launch_operations.data_parsing import DataParser
from src.launch_operations.launch_utils import get_default_args, get_default_op_name_stack, get_parameters
from src.launch_operations.options_appliers import OperationOptionsApplier, BranchOptionsApplier
from src.operation import Operation
from src.branch import Branch
from src.processor.one_op_processor import process_one_operation
from src.utils.formatters import LoggerBuilder


log = LoggerBuilder().build()


def run(branch_operations: Branch,
        init_data: Optional[Any] = None,
        distribute_args: Optional[Any] = None,
        config: Optional[str] = None) -> Any:

    print()
    print(11111111111111, init_data, distribute_args, branch_operations)

    config = Configurations(config) if isinstance(config, str) else config

    def_args = get_default_args(
        branch_operations, init_data, def_args, distribute_args)
    # name_stack, op_name = get_default_op_name_stack(op_name)
    param = get_parameters(branch_operations, name_stack)

    param, current_operation, def_args = BranchOptionsApplier(param).apply_options(init_data, distribute_args)
    param = OperationOptionsApplier(param, config).apply_options(init_data, distribute_args)

    # init_data, distribute_args = ArgsDistributor.get_init_data(init_data, distribute_args)

    print(22222222222222, init_data, param.name_stack, op_name, init_data, distribute_args, rw_inst, def_args)

    res = DataParser.get_result(
        init_data, distribute_args, def_args, rw_inst, op_name, param)
    if res is None:
        return None
    init_data, distribute_args, rw_inst, param = res

    print(20002222222200022222, init_data, distribute_args, rw_inst)
    init_data, param = create_fork_if_requred(
        param, op_name, init_data, rw_inst, def_args, distribute_args)

    # data_for_call, init_data = ArgsDistributor.get_data_for_call(
    #     param.flags.is_it_delay_return, init_data, distribute_args)

    print(333333333333333, init_data, param.name_stack, op_name, rw_inst, distribute_args)
    result, last_op_name, rem_args = process_one_operation(
        param.one_operation, init_data, rw_inst, name_stack=param.name_stack, op_name=op_name)


    print(444444444444444, result, init_data, last_op_name, rem_args, distribute_args)
    # rem_args, init_data, param = ArgsDistributor.fork_postprocessing(
    #     rem_args, init_data, param)

    result, rem_args, distribute_args, param = ArgsDistributor.get_args_for_next_operation(
        result, init_data, rem_args, distribute_args, last_op_name, rw_inst, param)

    if not len(param.operations):
        return result

    print(666666666666666, param)
    print(666666666666667, param.flags)
    print(666666666666668, result, last_op_name, rw_inst, rem_args, param.remaining_operations, def_args, distribute_args)
    return run(param,
               init_data=result,
               op_name=param.name_stack,
               rw_inst=rw_inst,
               distribute_args=distribute_args,
               all_operations_must_be_executed=all_operations_must_be_executed)


def create_fork_if_requred(
        par: LaunchParameters,
        op_name: Optional[str],
        init_data: Tuple,
        rw_inst: Dict[str, Any],
        def_args: Optional[Tuple],
        distribute_args: Optional[Tuple]) -> Tuple[Tuple, LaunchParameters]:
    if par.many_operations:
        op_name = f"{par.name_stack} -> {op_name}" if op_name else \
            f"{par.name_stack} -> BRANCH NAME NOT DEFINED"
        par.one_operation = create_operation_for_fork(
            par.many_operations, op_name, init_data, rw_inst,
            def_args, distribute_args, par.flags.all_operations_must_be_executed)
        return (), par
    return init_data, par


def create_operation_for_fork(
        operations: List[Any],
        op_name: Optional[str],
        init_data: Tuple,
        rw_inst: Dict[str, Any],
        def_args: Optional[Tuple],
        distribute_args: Optional[Tuple],
        all_operations_must_be_executed: bool) -> Dict:
    return {run: {
        "operations": operations,
        "init_data": init_data,
        "op_name": op_name,
        "rw_inst": rw_inst,
        "def_args": def_args,
        "distribute_args": distribute_args,
        "all_operations_must_be_executed": all_operations_must_be_executed}}
