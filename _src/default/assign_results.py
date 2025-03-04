from typing import Dict, Type

from _src.default.rw_classes import Variables, Values


def assign(*args, **kwargs):
    """Assign the result of the previous operation to the fields of special classes sequentially.

    Returned result:
        only instances of the special classes, with new values. No pos_args will be returned.
    """
    rw_instances: Dict[str, Type] = {}
    args = list(args)
    for par_str, dclass in kwargs.items():
        splited_par = par_str.split(".")
        rw_instances[dclass.__class__.__name__] = dclass
        if len(splited_par) > 1:
            result = dclass
            last_rw_inst, last_field = None, None
            for field in splited_par[1:]:
                last_rw_inst = result
                try:
                    result = result.__getattribute__(field)
                except AttributeError:
                    if isinstance(result, Variables) or isinstance(result, Values):
                        result = None
                last_field = field
            try:
                first_value = args.pop(0)
            except IndexError:
                raise ValueError("Not enough positional arguments to assign fields to special classes")

            last_rw_inst.__setattr__(last_field, first_value)

    return tuple(list(rw_instances.values()))
