from typing import List
import json

class SQLite3Table:
    table_name: str
    table_columns: List[str]
    table_datatypes: List[str]
    table_properties: List[str]

    def __init__(self, table_name: str, table_columns: List[str], table_datatypes: List[str], table_properties: List[str]) -> None:
        self.table_name = table_name
        self.table_columns = table_columns
        self.table_datatypes = table_datatypes
        self.table_properties = table_properties    
        
    @classmethod
    def from_json(cls, file_path):
        with open(file_path,'r') as fp:
            datas = json.load(fp)
        if isinstance(datas,list):
            return [cls(**data) for data in datas]
        elif isinstance(datas,dict):
            return cls(**datas)
        else:
            raise ValueError(f'Unknow Data Type for file path: {file_path}')
        
    @classmethod
    def from_json_str(cls, data):
        datas = json.loads(data)
        if isinstance(datas,list):
            return [cls(**data) for data in datas]
        elif isinstance(datas,dict):
            return cls(**datas)
        else:
            raise ValueError(f'Unknow Data Type for data: {data}')
        
    @classmethod
    def from_dict(cls, data):
        return cls(**data) 