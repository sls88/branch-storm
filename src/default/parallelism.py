from concurrent.futures import ThreadPoolExecutor
from typing import Any, List, Optional, Tuple, Sequence

from src.branch import Branch
from src.utils.formatters import LoggerBuilder

log = LoggerBuilder().build()


def check_sequence_lengths(*args):
    for arg in args:
        if len(arg) != len(args[0]):
            raise ValueError("The lengths of the sequences are not equal to each other")


def add_sequences(base_seq: Sequence, *sequences: Tuple[Sequence]) -> List[Tuple]:
    check_sequence_lengths(base_seq, *sequences)
    combined_seq = []
    for num, base_el in enumerate(base_seq):
        if not isinstance(base_seq[0], Sequence):
            base_el = (base_el,)
        combined_seq.append((*base_el, *list(zip(*sequences))[num]))
    return combined_seq


def set_val_for_all(len_objects: int, value: Any) -> List[Any]:
    return list(map(lambda x: value, range(len_objects)))


def create_init_data_sequence(
        len_obj: int,
        idata_for_all: Optional[Any] = None,
        idata_for_each: Tuple[Sequence] = None) -> List[Tuple]:
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
    results = []
    if threads == "max":
        threads = len(arg_seq)
    else:
        threads = int(threads)
    with ThreadPoolExecutor(max_workers=threads) as pool:
        print(list(zip(table_branches_seq, arg_seq)))
        for one_thread_result in pool.map(lambda x: x[0].run(x[1]), zip(table_branches_seq, arg_seq)):
            results.append(one_thread_result)
        log.info(f"ThreadPoolExecutor has finished processing in {threads} threads")

    return tuple(results)


def update_br_name(
        job_name: str,
        table_branches_seq: Sequence[Branch]) -> Sequence[Branch]:
    type_seq = type(table_branches_seq)
    table_branches_seq = list(table_branches_seq)
    for num, t_branch in enumerate(table_branches_seq):
        name = t_branch.get_br_name()
        new_name = f"{job_name} -> {name}"
        t_branch.set_br_name(new_name)
        table_branches_seq[num] = t_branch

    return type_seq(table_branches_seq)


def parallelize_table_branches(
        job_name: str,
        table_branches_seq: Sequence[Branch],
        threads: str,
        idata_for_all: Optional[Any] = None,
        idata_for_each: Tuple[Sequence] = None) -> None:
    table_branches_seq = update_br_name(job_name, table_branches_seq)
    arg_seq = create_init_data_sequence(len(table_branches_seq), idata_for_all, idata_for_each)
    thread_pool(arg_seq, table_branches_seq, threads=threads)
