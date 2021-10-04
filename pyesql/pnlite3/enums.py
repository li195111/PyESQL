from enum import Enum, IntEnum

class BaseMethod(IntEnum):
    CREATE = 0
    SELECT = 1
    INSERT = 2
    UPDATE = 3
    DELETE = 4
    DROP = 5
    ALTER = 6
    EXPLAIN = 7


class DBObj(IntEnum):
    DATABASE = 0
    TABLE = 1


class Method(IntEnum):
    FROM = 0
    INTO = 1
    IF = 2
    SET = 3
    WHERE = 4
    VALUES = 5
    INDEX = 6
    ON = 7
    CONCURRENTLY = 8
    AND = 9
    ORDER = 10
    BY = 11
    BETWEEN = 12
    LIMIT = 13

class Mark(IntEnum):
    NOT = 0
    EXISTS = 1
    UNIQUE = 2

class TableProperty(Enum):
    EMPTY = ""
    NOT_NULL = "NOT NULL"
    PRIMARY_KEY = "PRIMARY KEY"
    
class Order(IntEnum):
    DESC = 0
    ASC = 1