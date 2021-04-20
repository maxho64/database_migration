from oracle import Oracle
from postgres import Postgres

oracle = Oracle()

postgres = Postgres()


def add_data(schema_name, table_name):
    rows = oracle.get_table_data(schema_name, table_name)
    postgres.add_data(schema_name, table_name, rows)


def create_tables(schema_name, table_name):
    columns = oracle.get_table_metadata(schema_name, table_name)
    postgres.create_table(schema_name, table_name, columns)
    add_data(schema_name, table_name)


def create_primary_keys(schema_name, table_name):
    primary_keys = oracle.get_primary_keys(schema_name, table_name)
    if len(primary_keys):
        postgres.set_primary_key(schema_name, table_name, list(map(lambda key: key['column_name'], primary_keys)))


def create_foreign_keys(schema_name):
    foreign_keys = oracle.get_foreign_keys(schema_name)

    if len(foreign_keys):
        for key in foreign_keys:
            schema = key["owner"]
            table_name = key["table_name"]
            column_name = key["column_name"]
            reference_column = key['r_column_name']
            reference_table = f"{key['r_owner']}.{key['r_table_name']}"
            postgres.set_foreign_key(schema, table_name, column_name, reference_table, reference_column)


def create_unique_constraint(schema_name, table_name):
    unique_keys = oracle.get_unique_keys(schema_name, table_name)

    if len(unique_keys):
        constraint_name = unique_keys[0]['constraint_name']
        uniques = list(map(lambda key: key['column_name'], unique_keys))
        postgres.set_unique(schema_name, table_name, constraint_name, uniques)


def create_indexes(schema_name):
    indexes = oracle.get_indexes(schema_name)

    if len(indexes):
        for index in indexes:
            index_name = index['index_name']
            column = index['column_name']
            table_name = index['table_name']
            postgres.create_index(schema_name, index_name, table_name, column)


def create_sequences(schema_name):
    schema_name = schema_name.upper()
    sequences = oracle.get_sequences(schema_name)
    default_max_value = 9223372036854775807

    for sequence in sequences:
        sequence_name = sequence['sequence_name']
        start = sequence['last_number']
        min_val = sequence['min_value']
        max_val = sequence['max_value']
        max_val = default_max_value if max_val > default_max_value else max_val
        postgres.create_sequence(schema_name, sequence_name, start, min_val, max_val)


def create_triggers(schema_name):
    schema_name = schema_name.upper()
    triggers = oracle.get_triggers(schema_name)
    for trigger in triggers:
        name = trigger['trigger_name']
        event = trigger['triggering_event']
        body = trigger['script']
        trigger_object = f"{schema_name}.{trigger['object_name'].lower()}"
        postgres.create_trigger(name, event, body, trigger_object)


def create_database(schema_name):
    postgres.create_schema(schema_name)
    tables = oracle.get_tables_by_schema(schema_name)

    create_sequences(schema_name)

    for table in tables:
        schema_name = table['owner']
        table_name = table['table_name']

        create_tables(schema_name, table_name)

        create_primary_keys(schema_name, table_name)
        create_unique_constraint(schema_name, table_name)

    create_foreign_keys(schema_name)
    create_indexes(schema_name)
    # create_triggers(schema_name)


schemas = ["schema_name"]
for schema in schemas:
    create_database(schema)
