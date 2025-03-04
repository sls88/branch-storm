class IncorrectParameterError(Exception):
    pass


class EmptyBranchError(Exception):
    pass


class EmptyDataError(Exception):
    pass


class DistributionError(Exception):
    pass


class RemainingArgsFoundError(DistributionError):
    pass


class AssignmentError(Exception):
    pass
