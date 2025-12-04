from enum import Enum


class UnitStatus(Enum):
    EQUALS = 'igual'
    MOVED = 'movido'
    RENAMED = 'renombrado'
    DELETED = 'eliminado'
    NEW = 'nuevo'
