from typing import Dict, Type

from ..default.rw_classes import Variables, Values


def assign(*args, **kwargs):
    """Assign the result of the previous operation to the fields of special classes sequentially.

    Returned result:
        only instances of the special classes, with new values. No pos_args will be returned.
    """
    rw_instances: Dict[str, Type] = {}
    args_iter = iter(args)

    def next_value():
        try:
            return next(args_iter)
        except StopIteration:
            raise ValueError(
                "Not enough positional arguments "
                "to assign fields to special classes")

    for path, inst in kwargs.items():
        cls_name = inst.__class__.__name__
        parts = path.split(".")

        if len(parts) == 1:
            rw_instances[cls_name] = next_value()
            continue

        rw_instances[cls_name] = inst

        owner = inst
        last_owner = inst
        last_field = None

        for field in parts[1:]:
            last_owner = owner
            try:
                owner = getattr(owner, field)
            except AttributeError:
                if isinstance(last_owner, (Variables, Values)):
                    owner = None
            last_field = field

        value = next_value()
        last_owner.__setattr__(last_field, value)

    return tuple(rw_instances.values())
