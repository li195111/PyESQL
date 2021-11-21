from datetime import datetime
from .enums import *
from .postgre import PostgreTable
import psycopg2
from psycopg2 import OperationalError
from typing import Union

import warnings
warnings.filterwarnings("ignore", category=UserWarning)


class Database:
    def __init__(self, database=None, username=None, password=None, host=None,
                 create_db_if_notexists: bool = True) -> None:
        self.connect_string_list = []
        if host:
            _host_string = f"host={host}"
            self.connect_string_list.append(_host_string)
        if username:
            _user_string = f'user={username}'
            self.connect_string_list.append(_user_string)
        if password:
            _password_string = f'password={password}'
            self.connect_string_list.append(_password_string)
        self.connect_str = self.connect_str_without_db = ' '.join(
            self.connect_string_list)

        if database:
            self._build_connect_str_with_db(database)
        self._database_name = database

        if create_db_if_notexists and database:
            if not self.check_database_exists():
                self.create_database()

    @property
    def database_name(self):
        return self._database_name

    @database_name.setter
    def database_name(self, value):
        self._database_name = value
        self._build_connect_str_with_db(self._database_name)

    def _build_connect_str_with_db(self, database):
        _database_string = f'dbname={database}'
        connect_str_with_db = self.connect_string_list.copy()
        connect_str_with_db.append(_database_string)
        self.connect_str = ' '.join(connect_str_with_db)

    def _execute(self, sql, fetchall=False, autocommit=False, conn_str=None, timeout=3):
        conn = psycopg2.connect(self.connect_str if conn_str == None else conn_str, connect_timeout=timeout)
        if autocommit:
            conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql)
        if fetchall:
            res = cursor.fetchall()
        else:
            res = conn.commit()
        cursor.close()
        conn.close()
        return res


    def _to_sql_string(self, sql_list: list, end=True) -> str:
        sql = []
        for sql_item in sql_list:
            if not isinstance(sql_item, str):
                sql.append(sql_item.name)
            else:
                sql.append(sql_item)
        if end:
            return " ".join(sql) + ";"
        return " ".join(sql)

    def _condition_string(self, conditions, return_values=False):
        sql = [Method.WHERE]
        values = []
        for idx, (k, v) in enumerate(conditions.items()):
            if return_values:
                cond_sql = f"{k} = ?"
            else:
                cond_sql = f"{k}='{v}'"
            sql.append(cond_sql)
            if len(conditions) > 1 and idx != len(conditions)-1:
                sql.append(Method.AND)
            values.append(v)
        if return_values:
            return sql, values
        return sql

    def _itemsvalue_string(self, items, values):
        update_str = []
        for idx, (i, v) in enumerate(zip(items, values)):
            if idx > 0:
                update_str.append(",")
            update_str.append(f"{i}='{v}'")
        return update_str

    def _select_items_condition_sql(self, table: str = None, items: list = None, conditions: Union[dict, list] = None, base_method: BaseMethod = BaseMethod.SELECT) -> list:
        if isinstance(items, list) or isinstance(items, tuple):
            items = ','.join(items)
        sql = [base_method]
        if items:
            sql.append(items)
        if table:
            sql += [Method.FROM, table]
        if conditions != None:
            if isinstance(conditions, dict) and len(conditions) > 0:
                sql.extend(self._condition_string(
                    conditions, return_values=False))
            elif isinstance(conditions, list):
                sql.extend(conditions)
            else:
                raise ValueError("'conditions' value must be condition dict")
        return sql

    def _insert_item_sql(self, table, items: list, values: list, base_method: BaseMethod = BaseMethod.INSERT):
        if isinstance(items, str):
            pass
        if isinstance(items, list) or isinstance(items, tuple):
            items = ','.join(items)
        else:
            raise ValueError(
                "'item' value must be string or list of string or tuple")

        if isinstance(values[-1], list) or isinstance(values[-1], tuple):
            value_string = []
            for vals in values:
                value_string.append(
                    '(' + ','.join(["'"+str(val)+"'" for val in vals]) + ')')
            value_string = ','.join(value_string)
        else:
            value_string = str(tuple([str(val) for val in values]))

        sql = [base_method, Method.INTO, table,
               "("+items+")", Method.VALUES, value_string]
        return sql

    def _edit_database(self, base_method, db_name, args=None,  conn_str=None):
        sql = [base_method, DBObj.DATABASE]
        if args:
            sql += list(args)
        sql.append(db_name)
        return self._execute(self._to_sql_string(sql), autocommit=True, conn_str=conn_str)

    def _update_item(self, table, update_item: list, update_value: list, conditions: dict = None, base_method: BaseMethod = BaseMethod.UPDATE):
        sql = [base_method, table, Method.SET]
        update_str = self._itemsvalue_string(update_item, update_value)
        sql.extend(update_str)
        if isinstance(conditions, dict) and len(conditions) > 0:
            condition_sql = self._condition_string(
                conditions, return_values=False)
            sql.extend(condition_sql)
            self._execute(self._to_sql_string(sql))

    def _delete_item_condition(self, table, conditions: dict, base_method: BaseMethod = BaseMethod.DELETE):
        if isinstance(conditions, dict) and len(conditions) > 0:
            sql = [base_method, Method.FROM, table]
            condition_sql = self._condition_string(
                conditions, return_values=False)
            sql.extend(condition_sql)
            self._execute(self._to_sql_string(sql))
        else:
            raise ValueError(
                "'conditions' value must be condition dict and at least one condition")

    def list_database_name(self):
        return self.select_items("pg_database", "datname", conn_str=self.connect_str_without_db)

    def check_database_exists(self, db_name=None):
        if not db_name:
            db_name = self.database_name
        db_names = self.list_database_name()
        return tuple([db_name]) in db_names if not db_names is None else False

    def create_database(self, db_name=None) -> None:
        if not db_name:
            db_name = self.database_name
        if not self.check_database_exists(db_name):
            return self._edit_database(BaseMethod.CREATE, db_name=db_name, conn_str=self.connect_str_without_db)

    def drop_database(self, db_name=None):
        if not db_name:
            db_name = self.database_name
        if self.check_database_exists(db_name=db_name):
            return self._edit_database(BaseMethod.DROP, db_name=db_name, args=[Method.IF, Mark.EXISTS], conn_str=self.connect_str_without_db)

    def create_table(self, table_name=None, columns=None, datatypes=None, properties=None, postgretable: PostgreTable = None, base: BaseMethod = BaseMethod.CREATE):
        if postgretable:
            table_name = postgretable.table_name
            columns = postgretable.table_columns
            datatypes = postgretable.table_datatypes
            properties = postgretable.table_properties
        else:
            if not table_name and not columns and not datatypes and not properties:
                raise ValueError(
                    f"'table_name', 'columns', 'datatypes', 'properties' must not None, but got {table_name} {columns} {datatypes} {properties}")
        sql = [base, DBObj.TABLE]
        sql += [Method.IF, Mark.NOT, Mark.EXISTS]
        sql.append(table_name)
        format_strings = []
        for item, typ, desc in zip(columns, datatypes, properties):
            format_str = f"{item} {typ} {desc}"
            format_strings.append(format_str)
        formated_string = ",".join(format_strings)
        sql.append(f"({formated_string})")
        return self._execute(self._to_sql_string(sql))

    def drop_table(self, table_name, base: BaseMethod = BaseMethod.DROP):
        sql = [base, DBObj.TABLE]
        sql += [Method.IF, Mark.EXISTS]
        sql.append(table_name)
        return self._execute(self._to_sql_string(sql))

    def select_items(self, table: str, items: Union[str, list, tuple], conditions: dict = None, order_by: str = None, order: Order = Order.DESC, conn_str=None):
        sql = self._select_items_condition_sql(table, items, conditions)
        if order_by:
            sql.extend([Method.ORDER, Method.BY, order_by, order.name])
        return self._execute(self._to_sql_string(sql), fetchall=True, conn_str=conn_str)

    def select_counts_in_time(self, table: str, items, count_name, time_name, date_start:datetime, date_end:datetime, conn_str=None):
        sql = self._select_items_condition_sql(
            table, f'count({table}.{items})')
        sql.insert(2,f"as {count_name}")
        sql += [Method.WHERE, time_name, Method.BETWEEN, f"'{date_start}'", Method.AND, f"'{date_end}'"]
        return self._execute(self._to_sql_string(sql), fetchall=True, conn_str=conn_str)

    def insert_item(self, table, items, values):
        sql = self._insert_item_sql(table, items, values)
        self._execute(self._to_sql_string(sql))

    def update_item(self, table: str, update_item: list, update_value: list, conditions: dict = None):
        self._update_item(table, update_item, update_value, conditions)
        return

    def delete_item(self, table: str, conditions: dict):
        return self._delete_item_condition(table, conditions)

    def create_index(self, index_name, table_name, column_names: Union[str, list, tuple], unlock=True, base: BaseMethod = BaseMethod.CREATE):
        sql = [base, Method.INDEX]
        if unlock:
            sql.append(Method.CONCURRENTLY)
        sql += [index_name, Method.ON, table_name]
        if isinstance(column_names, str):
            sql.append(f"({column_names})")
        if isinstance(column_names, list) or isinstance(column_names, tuple):
            sql.append(f"({','.join(column_names)})")
        return sql
