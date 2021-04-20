import cx_Oracle
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class Oracle(object):
    DEFAULT_IPADDR = '127.0.0.1'
    DEFAULT_PORT = 1521
    DEFAULT_SID = 'dn'
    DEFAULT_LOGIN = 'sys'
    DEFAULT_PASSWORD = 'pass'

    def __init__(self, address=DEFAULT_IPADDR, port=DEFAULT_PORT, sid=DEFAULT_SID, login=DEFAULT_LOGIN,
                 password=DEFAULT_PASSWORD, mode=cx_Oracle.SYSDBA):
        dsn = cx_Oracle.makedsn(address, port, sid)

        self.engine = create_engine(
            f"oracle+cx_oracle://{login}:{password}@{dsn}",
            connect_args={
                "encoding": "UTF-8",
                "nencoding": "UTF-8",
                "mode": mode,
                "events": True
            }
        )

        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def get_session(self):
        return self.session

    def get_engine(self):
        return self.engine

    def __get_many_data(self, query):
        return self.session.execute(query).fetchall()

    def get_tables_by_schema(self, schema):
        query = f"SELECT * FROM all_tables WHERE OWNER = '{schema.upper()}' ORDER BY table_name"
        return self.__get_many_data(query)

    def get_table_metadata(self, schema, table):
        query = f"select * from all_tab_columns where table_name = '{table}' and owner = '{schema.upper()}'"
        columns = self.__get_many_data(query)
        return columns

    def get_table_data(self, schema, table):
        query = f"SELECT * FROM {schema}.{table}"
        return self.__get_many_data(query)

    def get_indexes(self, schema):
        query = (f"select index_name, column_name, table_name from dba_ind_columns "
                 f"where table_owner='{schema}'")
        return self.__get_many_data(query)

    def get_foreign_keys(self, schema_name):
        query = (f"SELECT a.table_name, a.column_name, a.constraint_name, c.owner, "
                 f"c.r_owner, c_pk.table_name r_table_name, c_pk.constraint_name r_pk, col.column_name r_column_name "
                 f"FROM all_cons_columns a "
                 f"JOIN all_constraints c ON a.owner = c.owner AND a.constraint_name = c.constraint_name "
                 f"JOIN all_constraints c_pk ON c.r_owner = c_pk.owner AND c.r_constraint_name = c_pk.constraint_name "
                 f"JOIN dba_cons_columns col on c.r_owner = col.owner and  c_pk.constraint_name = col.constraint_name "
                 f"WHERE c.constraint_type = 'R' AND a.owner = '{schema_name}'")
        return self.__get_many_data(query)

    def get_constraint_by_type(self, schema_name, table_name, constraint_type="P"):
        query = (f"SELECT cons.constraint_name, cols.table_name, cols.column_name, cols.position, cons.status, cons.owner "
                 f"FROM all_constraints cons, all_cons_columns cols "
                 f"WHERE cols.table_name = '{table_name}' "
                 f"AND cols.owner = '{schema_name}' AND cons.constraint_type = '{constraint_type}' "
                 f"AND cons.constraint_name = cols.constraint_name "
                 f"AND cons.owner = cols.owner ORDER BY cols.table_name, cols.position")
        return self.__get_many_data(query)

    def get_triggers(self, schema_name):
        query = (f"select owner as trigger_schema_name, trigger_name, trigger_type, triggering_event, "
                 f"table_owner as schema_name, table_name as object_name, base_object_type as object_type, status, "
                 f"trigger_body as script from sys.dba_triggers where owner = '{schema_name}'")
        return self.__get_many_data(query)

    def get_primary_keys(self, schema_name, table_name):
        return self.get_constraint_by_type(schema_name, table_name, "P")

    def get_unique_keys(self, schema_name, table_name):
        return self.get_constraint_by_type(schema_name, table_name, "U")

    def get_sequences(self, schema_name):
        query = f"select * from dba_sequences where sequence_owner = '{schema_name}'"
        return self.__get_many_data(query)

