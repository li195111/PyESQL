from enum import Enum

class BaseMethod(Enum):
    CREATE = 0
    SELECT = 1
    INSERT = 2
    UPDATE = 3
    DELETE = 4
    DROP = 5
    ALTER = 6
    EXPLAIN = 7


class DBObj(Enum):
    DATABASE = 0
    TABLE = 1


class Method(Enum):
    FROM = 0
    INTO = 1
    IF = 2
    SET = 3
    WHERE = 4
    VALUES = 5
    INDEX = 6
    ON = 7
    CONCURRENTLY = 8


class Mark(Enum):
    NOT = 0
    EXISTS = 1

class TableProperty(Enum):
    EMPTY = ""
    NOT_NULL = "NOT NULL"
    PRIMARY_KEY = "PRIMARY KEY"