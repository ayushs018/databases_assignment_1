from sqlalchemy import create_engine, Table, Column, String, MetaData

engine = create_engine("sqlite:///adaptive.db")
metadata_obj = MetaData()
tables = {}

def get_sql_table(fields):
    
    global tables

    key = tuple(sorted(fields))
    if key in tables:
        return tables[key]

    columns = [Column("sys_ingested_at", String, primary_key=True), Column("username", String)]

    for f in fields:
        if f not in ("sys_ingested_at", "username"):
            columns.append(Column(f, String))

    table = Table("records", metadata_obj, *columns, extend_existing=True)
    metadata_obj.create_all(engine)
    tables[key] = table
    return table


def insert_sql(record, sql_fields):
    table = get_sql_table(sql_fields)

    insert_data = {k: str(record.get(k)) for k in sql_fields if k in record}

    with engine.begin() as conn:
        conn.execute(table.insert().values(**insert_data))