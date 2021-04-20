import base64

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


class Postgres(object):
    DEFAULT_IPADDR = '127.0.0.1'
    DEFAULT_PORT = 5432
    DEFAULT_BASE = 'db'
    DEFAULT_LOGIN = 'postgres'
    DEFAULT_PASSWORD = 'postgres'

    def __init__(self, address=DEFAULT_IPADDR, port=DEFAULT_PORT, database=DEFAULT_BASE, login=DEFAULT_LOGIN,
                 password=DEFAULT_PASSWORD):
        self.engine = create_engine(f'postgresql://{login}:{password}@{address}:{port}/{database}')

        Base = declarative_base()
        Base.metadata.bind = self.engine
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.sql_show = False

    def show_sql(self, show):
        self.sql_show = show

    def get_session(self):
        return self.session

    def add_data(self, schema_name, table_name, rows):
        try:
            for row in rows:
                self.insert_row(schema_name, table_name, row)
            self.session.commit()
        except Exception as ex:
            print(ex)
            self.session.rollback()

    def execute_query(self, query):
        if self.sql_show:
            print(query)
        try:
            self.session.execute(query)
            self.session.commit()
        except Exception as ex:
            print(ex)
            self.session.rollback()

    def create_schema(self, schema_name):
        query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
        self.execute_query(query)

    def create_table(self, schema_name, table_name, columns):
        query = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ("
        for index, column in enumerate(columns):
            postgres_type = self.convert_type(column["data_type"], column['char_length'])
            nullable = self.get_nullable(column["nullable"])
            query += f"{column['column_name']} {postgres_type} {nullable}"
            if index < len(columns) - 1:
                query += ",\n"
        query += ");"
        self.execute_query(query)

    def insert_row(self, schema, table_name, row):
        values = map(self.__parse_value, row.values())
        columns_string = ", ".join(row.keys())
        values_string = ", ".join(values)

        query = f"INSERT INTO {schema}.{table_name.lower()} ({columns_string}) VALUES ({values_string})"

        self.session.execute(query)

    def set_primary_key(self, schema, table_name, keys):
        key = ", ".join(keys)
        query = f"ALTER TABLE {schema}.{table_name.lower()} ADD PRIMARY KEY ({key})"
        self.execute_query(query)

    def set_foreign_key(self, schema, table_name, column_name, reference_table, reference_column):
        query = (f"ALTER TABLE {schema}.{table_name.lower()} ADD FOREIGN KEY ({column_name}) "
                 f"REFERENCES {reference_table}({reference_column})")
        self.execute_query(query)

    def set_unique(self, schema, table_name, constraint_name, columns):
        unique_columns = ", ".join(columns)
        query = f'ALTER TABLE {schema}.{table_name.lower()} ADD CONSTRAINT {constraint_name} UNIQUE ({unique_columns})'
        self.execute_query(query)

    def create_sequence(self, schema, sequence_name, start, min_val, max_val):
        query = f'CREATE SEQUENCE IF NOT EXISTS  {schema}.{sequence_name} MINVALUE {min_val} MAXVALUE {max_val} START {start}'
        self.execute_query(query)

    def create_trigger(self, name, event, body, object):
        query = (
            f"CREATE FUNCTION {name}() RETURNS trigger AS ${name}$  {body}; ${name}$ LANGUAGE plpgsql; "
            f"CREATE TRIGGER {name} BEFORE {event} ON {object} "
            f"FOR EACH ROW EXECUTE PROCEDURE {name}();")
        print(query)
        #self.execute_query(query)

    def __parse_value(self, value):
        if value is None:
            return "NULL"

        if type(value) in [int, float]:
            return f"{value}"
        elif type(value) == bytes:
            return f"'{base64.b64encode(value).decode('utf-8')}'"
        else:
            value = self.__escapa_string(value) if type(value) == str else value
            return f"'{value}'"

    def create_index(self, schema, index_name, table_name, column):
        query = f'CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON  {schema}.{table_name.lower()}  ({column}) '
        self.execute_query(query)

    def __escapa_string(self, value):
        return value.replace("'", "''").replace(":", ": ").replace("  ", " ")

    def convert_type(self, oracle_type, length):
        if oracle_type == 'NUMBER':
            return 'numeric'
        elif oracle_type == 'BLOB':
            return 'bytea'
        elif oracle_type == 'CLOB':
            return 'TEXT'
        elif oracle_type == 'VARCHAR2':
            return f'varchar ({length})'

        return oracle_type

    def get_nullable(self, value):
        return "NOT NULL" if value == "N" else str()
