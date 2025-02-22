from typing import Any, Optional, Tuple, Dict, List, Union

# # from src.dataclasses import LaunchParameters
# from src.operation import Operation
# from src.branch import Branch


def to_tuple(data: Any) -> Tuple:
    return (data,) if not isinstance(data, Tuple) else data


# def get_stack_name(name_stack: str, op_name: Optional[str]) -> str:
#     if op_name is None:
#         return name_stack
#     return f"{name_stack} -> {op_name}"


# def get_default_op_name_stack(op_name: Optional[str]) -> Tuple[str, None]:
#     name_stack = op_name
#     if name_stack is None:
#         return "BRANCH NAME NOT DEFINED", None
#     return name_stack, None


# def get_parameters(
#         operations: Union[Branch, Operation, LaunchParameters],
#         name_stack: Optional[str]) -> LaunchParameters:
#     if isinstance(operations, LaunchParameters):
#         operations.name_stack = name_stack
#         return operations
#     return LaunchParameters(operations=operations, name_stack=name_stack)
#
#
# def get_default_args(
#         operations: Union[Branch, LaunchParameters],
#         init_data: Optional[Any],
#         def_args: Optional[Tuple],
#         distribute_args: Optional[Any]) -> Optional[Tuple]:
#     return () if not isinstance(operations, LaunchParameters) and \
#                  init_data is None and \
#                  def_args is None and \
#                  distribute_args is None else def_args
