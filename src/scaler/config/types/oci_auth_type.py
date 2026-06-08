import enum


class OCIAuthType(enum.Enum):
    config_file = enum.auto()
    instance_principal = enum.auto()
