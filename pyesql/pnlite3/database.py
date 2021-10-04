import contextlib
import sqlite3
from datetime import datetime
from sqlite3 import Error
from typing import List, Union

from .enums import BaseMethod, DBObj, Mark, Method, Order
from .lite3 import SQLite3Table


class Database:
    def __init__(self, file_path, in_memory=False) -> None:
        self._database_name = file_path
        if not file_path or in_memory:
            self._database_name = ':memory:'
        
    @property
    def database_name(self):
        return self._database_name

    @database_name.setter
    def database_name(self, value):
        self._database_name = value

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
    
    def _execute(self, sql, fetchall=False, fetchone=False,nocommit=False):
        try:
            with contextlib.closing(sqlite3.connect(self.database_name)) as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                if nocommit:
                    return
                if fetchall:
                    return cursor.fetchall()
                if fetchone:
                    return cursor.fetchone()
                return conn.commit()
        except Error as e:
            print (e,f"\nSQL: {sql}") 
            
    def _execute_value(self, sql, values, fetchall=False):
        try:
            with contextlib.closing(sqlite3.connect(self.database_name)) as conn:
                cursor = conn.cursor()
                cursor.execute(sql, values)
                if fetchall:
                     return cursor.fetchall()
                return conn.commit()
        except Error as e:
            print (e,f"\nSQL: {sql}") 
            
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
    
    def _update_item(self, table, update_item: list, update_value: list, conditions: dict = None, base_method: BaseMethod = BaseMethod.UPDATE):
        sql = [base_method, table, Method.SET]
        update_str = self._itemsvalue_string(update_item, update_value)
        sql.extend(update_str)
        if isinstance(conditions, dict) and len(conditions) > 0:
            condition_sql = self._condition_string(
                conditions, return_values=False)
            sql.extend(condition_sql)
            self._execute(self._to_sql_string(sql))

    def _delete_item_condition(self, table, conditions: Union[dict,List], compare_list=False, base_method: BaseMethod = BaseMethod.DELETE):
        if compare_list:
            if isinstance(conditions, list) and len(conditions) > 2:
                sql = [base_method, Method.FROM, table, Method.WHERE] + [f'{cond}' for cond in conditions]
                return sql
            else:
                raise ValueError(
                    "'conditions' value must be condition list and at least 3 parameters condition when 'compare_list = True'")
        else:
            if isinstance(conditions, dict) and len(conditions) > 0:
                sql = [base_method, Method.FROM, table]
                condition_sql = self._condition_string(
                    conditions, return_values=False)
                sql.extend(condition_sql)
                return sql
            else:
                raise ValueError(
                    "'conditions' value must be condition dict and at least one condition")

    def create_table(self, table_name=None, columns=None, datatypes=None, properties=None, 
                     sqlite3table: SQLite3Table = None, 
                     base: BaseMethod = BaseMethod.CREATE):
        if sqlite3table:
            table_name = sqlite3table.table_name
            columns = sqlite3table.table_columns
            datatypes = sqlite3table.table_datatypes
            properties = sqlite3table.table_properties
        else:
            if not table_name and not columns and not datatypes and not properties:
                raise ValueError(
                    f"'table_name', 'columns', 'datatypes', 'properties' must not None, but got {table_name} {columns} {datatypes} {properties}")
        sql = [base, DBObj.TABLE, Method.IF, Mark.NOT, Mark.EXISTS, table_name]
        format_strings = []
        for item, typ, desc in zip(columns, datatypes, properties):
            format_str = f"{item} {typ} {desc}"
            format_strings.append(format_str)
        formated_string = ",".join(format_strings)
        sql += [f"({formated_string})"]
        return self._execute(self._to_sql_string(sql))

    def drop_table(self, table_name, base: BaseMethod = BaseMethod.DROP):
        sql = [base, DBObj.TABLE, table_name]
        return self._execute(self._to_sql_string(sql))

    def select_items(self, table: str, items: Union[str, list, tuple], conditions: dict = None, 
                     order_by: str = None, order: Union[Order,str] = 'desc',
                     limit: int = None):
        sql = self._select_items_condition_sql(table, items, conditions)
        if limit:
            sql.extend([Method.ORDER, Method.BY, order_by, order, Method.LIMIT, f"{limit}"])
            return self._execute(self._to_sql_string(sql), fetchone=True)
        elif order_by:
            sql.extend([Method.ORDER, Method.BY, order_by, order])
        return self._execute(self._to_sql_string(sql), fetchall=True)
    
    def select_counts_in_time(self, table: str, items, count_name, time_name, date_start:datetime, date_end:datetime):
        sql = self._select_items_condition_sql(
            table, f'count({table}.{items})')
        sql.insert(2,f"as {count_name}")
        sql += [Method.WHERE, time_name, Method.BETWEEN, f"'{date_start}'", Method.AND, f"'{date_end}'"]
        return self._execute(self._to_sql_string(sql), fetchall=True)

    def insert_item(self, table, items, values):
        sql = self._insert_item_sql(table, items, values)
        self._execute(self._to_sql_string(sql))
        
    def delete_item(self, table: str, conditions: Union[dict,List], compare_list=False):
        sql = self._delete_item_condition(table, conditions, compare_list)
        return self._execute(self._to_sql_string(sql))
    
    def update_item(self, table: str, update_item: list, update_value: list, conditions: dict = None):
        self._update_item(table, update_item, update_value, conditions)
        return
    
    def create_index(self, index_name, table_name, column_names: Union[str, list, tuple], unique=False, base:BaseMethod=BaseMethod.CREATE):
        sql = [base]
        if unique:
            sql.append(Mark.UNIQUE)
        sql += [Method.INDEX, Method.IF, Mark.NOT, Mark.EXISTS]
        sql += [index_name, Method.ON, table_name]
        if isinstance(column_names, str):
            sql.append(f"({column_names})")
        if isinstance(column_names, list) or isinstance(column_names, tuple):
            sql.append(f"({','.join(column_names)})")
        return self._execute(self._to_sql_string(sql),nocommit=True)

    def custom_SQL(self,sql_string,fetchall=True,fetchone=False):
        return self._execute(sql_string,fetchall=fetchall,fetchone=fetchone)

if __name__ == "__main__":
    # Create Database
    db = Database('camera_result.db')
    table_name = 'camera_result'
    columns = ['id','camera_source','result','create_date_time']
    datatypes = ['integer','text','text','timestamp']
    properties = ['PRIMARY KEY','NOT NULL','','NOT NULL']
    
    db.create_table(table_name, columns, datatypes, properties)
    values = [['0','{"0": 3, "1": 4, "2": 8}',datetime.utcnow()]] * 10
    # for i in range(10):
    #     sql = db.insert_item(table_name, columns[1:], values)
    items = db.select_items(table_name, columns)
    print (len(items))
    db.delete_item(table_name, ['id','<',300], compare_list=True)
    
    # db.drop_table(table_name)
    pass
