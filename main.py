import psycopg2


class Database:
    def __init__(self, db_name: str, db_user: str, db_password: str, db_host: str, db_port: str):
        try:
            self.conn = psycopg2.connect(
                database=db_name,
                user=db_user,
                password=db_password,
                host=db_host,
                port=db_port
            )
            self.cursor = self.conn.cursor()
        except Exception as e:
            raise e

    def create(self, table: str, data: dict) -> bool:
        columns = ', '.join(data.keys())
        values = ', '.join(data.values())
        sql = (f'INSERT INTO {table} ({columns}) VALUES ({values})'
               f'ON CONFLICT ({columns}) DO NOTHING')
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(e)
            return False

    def fetch(self, table: str, columns: list) -> list:
        sql = f"SELECT {', '.join(columns)} FROM {table}"
        try:
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except Exception as e:
            self.conn.rollback()
            print(e)
            return []
        
    def not_null(self, table: str, column: str) -> bool:
        sql = f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL;"
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(e)
            return False
    
    def unique(self, table: str, columns: list) -> bool:
        unique_columns = ', '.join(columns)
        sql = f"ALTER TABLE {table} ADD CONSTRAINT unique_constraint UNIQUE ({unique_columns});"
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(e)
            return False
    
    def column_type(self, table: str, column: str, new_type: str) -> bool:
        sql = f"ALTER TABLE {table} ALTER COLUMN {column} TYPE {new_type};"
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(e)
            return False
        
    def rename_column(self, table: str, old_column: str, new_column: str) -> bool:
        sql = f"ALTER TABLE {table} RENAME COLUMN {old_column} TO {new_column};"
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(e)
            return False
    
    def drop_column(self, table: str, column: str) -> bool:
        sql = f"ALTER TABLE {table} DROP COLUMN {column};"
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(e)
            return False
    
    def drop_table(self, table: str) -> bool:
        sql = f"DROP TABLE {table};"
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(e)
            return False

    def rename_table(self, old_table: str, new_table: str) -> bool:
        sql = f"ALTER TABLE {old_table} RENAME TO {new_table};"
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(e)
            return False
        
    def delete(self,table,cond,value):
        sql = "DELETE FROM {table} WHERE {cond} = {value}"
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(e)
            return False
        
    def update(self,table:str,data:str,value,condition:str,con_value:str):
        sql = f"UPDATE {table} SET {data}={value} WHERE {condition} = {con_value};"
        try:
            values = list(data.values()) + list(condition.values())
            self.cursor.execute(sql, values)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(e)
            return False
        
    def __del__(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    

# table=pupils,
# data = {
#     'id': 1,
#     'first_name': 'John',
#     'last_name': 'Smith',
# }
