class IncorrectParameterError(Exception):
    pass


class EmptyDataError(Exception):
    pass


class DistributionError(Exception):
    pass


class RemainingArgsFoundError(DistributionError):
    pass


class AssignmentError(Exception):
    pass


class ConditionNotMetError(Exception):
    pass
