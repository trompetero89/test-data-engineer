import avro.schema
import avro.datafile
import avro.io
import io
import psycopg2
import json

# backup a table to Avro format
def backup_table(table_name, conn, schema):
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    # Parse the schema
    parsed_schema = avro.schema.parse(json.dumps(schema))

    with open(f"{table_name}.avro", "wb") as out:
        writer = avro.io.DatumWriter(parsed_schema)
        datum_writer = avro.datafile.DataFileWriter(out, writer, parsed_schema)
        for row in rows:
            if table_name == 'hired_employees':
                row = (
                    row[0], 
                    row[1] if row[1] is not None else "",  
                    row[2] if row[2] is not None else "",  
                    int(row[3]) if row[3] is not None else -1,  
                    int(row[4]) if row[4] is not None else -1  
                )
            datum_writer.append(dict(zip([field["name"] for field in schema["fields"]], row)))
        datum_writer.close()
    cursor.close()

conn = psycopg2.connect(
    dbname="postgresdb",
    user="newuser",
    password="password",
    host="localhost",
    port="5432"
)

# Define schemas
hired_employees_schema = {
    "type": "record",
    "name": "hired_employees",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "datetime", "type": "string"},
        {"name": "department_id", "type": "int"},
        {"name": "job_id", "type": "int"}
    ]
}

departments_schema = {
    "type": "record",
    "name": "departments",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "department", "type": "string"}
    ]
}

jobs_schema = {
    "type": "record",
    "name": "jobs",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "job", "type": "string"}
    ]
}

# Backup the tables to Avro format
backup_table("hired_employees", conn, hired_employees_schema)
backup_table("departments", conn, departments_schema)
backup_table("jobs", conn, jobs_schema)

# Close the database connection
conn.close()