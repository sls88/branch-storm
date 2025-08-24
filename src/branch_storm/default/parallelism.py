from concurrent.futures import ThreadPoolExecutor
from typing import Any, List, Optional, Tuple, Sequence

from .rw_classes import RunConfigurations
from ..branch import Branch
from ..utils.formatters import LoggerBuilder

log = LoggerBuilder().build()


def check_sequence_lengths(*args):
    for arg in args:
        if len(arg) != len(args[0]):
            raise ValueError(
                "The lengths of the sequences "
                "are not equal to each other")


def add_sequences(
        base_seq: Sequence,
        *sequences: Tuple[Sequence]) -> List[Tuple]:
    """Concatenate per-index elements from multiple sequences into tuples.

    This function zips several sequences **by index** and builds a list of tuples.
    The leftmost sequence (`base_seq`) may already contain tuple-like elements;
    if it does not, each element will be wrapped into a 1-tuple before extending
    with the rest of the per-index values.

    Lengths of all sequences must match. Validation is delegated to
    `check_sequence_lengths`.

    Args:
      base_seq: The base sequence; elements can be scalars or sequences (e.g. tuples).
      *sequences: Additional sequences to append per index. Each must have the same
        length as `base_seq`.

    Returns:
      A list of tuples with length `len(base_seq)`. At index `i`, the tuple is:
      `tuple(base_element_i) + (seq1[i], seq2[i], ...)`. If `base_element_i` is not
      a sequence, it is wrapped as `(base_element_i,)`.

    Raises:
      ValueError: If the sequences have different lengths (as determined by
        `check_sequence_lengths`).

    Examples:
      >>> add_sequences([1, 3], [10, 20], ["a", "b"])
      [(1, 10, 'a'), (3, 20, 'b')]

      >>> add_sequences([(1, 2), (3, 4)], [10, 20])
      [(1, 2, 10), (3, 4, 20)]
    """
    check_sequence_lengths(base_seq, *sequences)
    combined_seq = []
    for num, base_el in enumerate(base_seq):
        if not isinstance(base_seq[0], Sequence):
            base_el = (base_el,)
        combined_seq.append((*base_el, *list(zip(*sequences))[num]))
    return combined_seq


def set_val_for_all(len_objects: int, value: Any) -> List[Any]:
    """Create a list filled with the same value.

    Args:
      len_objects: Target length of the list.
      value: Value to repeat.

    Returns:
      A list of length `len_objects` where each element equals `value`.

    Examples:
      >>> set_val_for_all(3, 42)
      [42, 42, 42]
    """
    return list(map(lambda x: value, range(len_objects)))


def create_init_data_sequence(
        len_obj: int,
        idata_for_all: Optional[Any] = None,
        idata_for_each: Tuple[Sequence] = None) -> List[Tuple]:
    """Build a per-branch input sequence of tuples for parallel execution.

    Produces a list of tuples of length `len_obj`. Each tuple represents the
    per-branch input argument(s). You can supply:
    - `idata_for_all`: a single value or a sequence applied to **every** branch.
    - `idata_for_each`: a tuple of sequences (one per additional argument stream),
      where each sequence has length `len_obj`.

    The final tuple for index `i` is formed by concatenating:
    `tuple(idata_for_all_as_tuple) + tuple(seq_j[i] for each seq_j in idata_for_each)`.

    Args:
      len_obj: Number of branches / desired result length.
      idata_for_all: Either:
        - `None` (no common prefix),
        - a scalar (will become a single-element tuple for every branch),
        - or a sequence to reuse as-is for every branch.
      idata_for_each: Tuple of per-branch sequences (one sequence per additional
        argument stream). Each sequence must have length `len_obj`.

    Returns:
      A list of tuples of length `len_obj`. Each tuple contains the consolidated
      inputs for the corresponding branch.

    Examples:
      # No inputs at all:
      >>> create_init_data_sequence(3)
      [(), (), ()]

      # One scalar applied to all:
      >>> create_init_data_sequence(3, idata_for_all=42)
      [(42,), (42,), (42,)]

      # A sequence applied to all:
      >>> create_init_data_sequence(2, idata_for_all=("db", "table"))
      [('db', 'table'), ('db', 'table')]

      # Only per-branch streams:
      >>> create_init_data_sequence(2, idata_for_each=([1, 2], ["a", "b"]))
      [(1, 'a'), (2, 'b')]

      # Both a scalar for all and per-branch streams:
      >>> create_init_data_sequence(2, idata_for_all=100, idata_for_each=([1, 2],))
      [(100, 1), (100, 2)]
    """
    if idata_for_all is None and idata_for_each is None:
        return set_val_for_all(len_obj, ())

    elif not isinstance(idata_for_all, Sequence) and idata_for_each is None:
        return set_val_for_all(len_obj, (idata_for_all,))

    elif isinstance(idata_for_all, Sequence) and idata_for_each is None:
        return set_val_for_all(len_obj, tuple(idata_for_all))

    elif idata_for_all is None and idata_for_each:
        id_for_all = set_val_for_all(len_obj, ())
        return add_sequences(id_for_all, *idata_for_each)

    elif not isinstance(idata_for_all, Sequence) and idata_for_each:
        id_for_all = set_val_for_all(len_obj, (idata_for_all,))
        return add_sequences(id_for_all, *idata_for_each)

    id_for_all = set_val_for_all(len_obj, tuple(idata_for_all))
    return add_sequences(id_for_all, *idata_for_each)


def thread_pool(
        arg_seq: Any,
        table_branches_seq: Sequence[Branch],
        threads: str = "max") -> Tuple:
    """Execute branches in parallel using a thread pool.

    Each branch is paired with its corresponding input from `arg_seq`
    (typically produced by `create_init_data_sequence`). The function calls
    `branch.run(arg)` for each pair, collecting results in order.

    Args:
      arg_seq: Iterable of per-branch inputs; each element is passed to
        `Branch.run`. Commonly a list of tuples from `create_init_data_sequence`.
      table_branches_seq: Sequence of `Branch` instances to run.
      threads: Either `"max"` (use `len(arg_seq)` workers) or a stringified integer
        specifying exact thread count.

    Returns:
      A tuple of results returned by each `Branch.run`, in the same order as input.

    Raises:
      ValueError: If `threads` is not `"max"` and cannot be converted to `int`.

    Examples:
      >>> branches = [br("A")[...], br("B")[...]]
      >>> args = create_init_data_sequence(len(branches), idata_for_each=([10, 20],))
      >>> thread_pool(args, branches, threads="2")
      (<result_of_A>, <result_of_B>)
    """
    results = []
    if threads == "max":
        threads = len(arg_seq)
    else:
        threads = int(threads)
    with ThreadPoolExecutor(max_workers=threads) as pool:
        for one_thread_result in pool.map(
                lambda x: x[0].run(x[1]),
                zip(table_branches_seq, arg_seq)):
            results.append(one_thread_result)
        log.info(f"ThreadPoolExecutor has finished "
                 f"processing in {threads} threads")

    return tuple(results)


def parallelize_without_result(
        run_config: RunConfigurations,
        table_branches_seq: Sequence[Branch],
        threads: str,
        idata_for_all: Optional[Any] = None,
        idata_for_each: Tuple[Sequence] = None) -> None:
    """Run branches in parallel and discard results.

    For each branch, a **renewed** `RunConfigurations` instance is created via
    `run_config.get_renewed_self_instance()` and injected with `.rw_inst({"run_conf": ...})`
    to ensure per-branch isolation. Initial arguments are assembled by
    `create_init_data_sequence`, then dispatched via `thread_pool`.

    Args:
      run_config: Base run configuration to clone per branch.
      table_branches_seq: Sequence of `Branch` instances.
      threads: `"max"` or a stringified integer for thread count.
      idata_for_all: Common input (scalar or sequence) prefixed to each branch input.
      idata_for_each: Tuple of per-branch sequences (must match number of branches).

    Returns:
      None.

    Examples:
      >>> rc = RunConfigurations()
      >>> branches = [br("One")[...], br("Two")[...], br("Three")[...]]
      >>> parallelize_without_result(
      ...     rc,
      ...     branches,
      ...     threads="max",
      ...     idata_for_each=([1, 2, 3],)   # one per-branch int
      ... )
      # Executes all three branches in parallel; results are ignored.
    """
    branches = []
    for branch in table_branches_seq:
        renewed_run_conf = run_config.get_renewed_self_instance()
        branches.append(branch.rw_inst({"run_conf": renewed_run_conf}))
    table_branches_seq = branches
    arg_seq = create_init_data_sequence(
        len(table_branches_seq), idata_for_all, idata_for_each)
    thread_pool(arg_seq, table_branches_seq, threads=threads)


def parallelize_with_result_return(
        run_config: RunConfigurations,
        table_branches_seq: Sequence[Branch],
        threads: str,
        idata_for_all: Optional[Any] = None,
        idata_for_each: Tuple[Sequence] = None) -> Tuple:
    """Run branches in parallel and return their results.

    Similar to `parallelize_without_result`, but collects and returns the results
    from each branch.

    Args:
      run_config: Base run configuration to clone per branch.
      table_branches_seq: Sequence of `Branch` instances.
      threads: `"max"` or a stringified integer for thread count.
      idata_for_all: Common input (scalar or sequence) prefixed to each branch input.
      idata_for_each: Tuple of per-branch sequences (must match number of branches).

    Returns:
      A tuple of results from each branch, in the same order as `table_branches_seq`.

    Examples:
      >>> rc = RunConfigurations()
      >>> branches = [br("left")[...], br("right")[...]]
      >>> args = create_init_data_sequence(
      ...     len(branches),
      ...     idata_for_all=("prefix",),                  # common argument
      ...     idata_for_each=([10, 20], ["x", "y"])       # per-branch streams
      ... )
      >>> parallelize_with_result_return(
      ...     rc, branches, threads="2",
      ...     idata_for_all=("prefix",),
      ...     idata_for_each=([10, 20], ["x", "y"])
      ... )
      (<left_result>, <right_result>)
    """
    branches = []
    for branch in table_branches_seq:
        branches.append(branch.rw_inst(
            {"run_conf": run_config.get_renewed_self_instance()}))
    table_branches_seq = branches
    arg_seq = create_init_data_sequence(
        len(table_branches_seq), idata_for_all, idata_for_each)
    return thread_pool(arg_seq, table_branches_seq, threads=threads)
