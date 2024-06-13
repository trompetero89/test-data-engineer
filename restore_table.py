import avro.schema
import avro.datafile
import avro.io
import psycopg2
import json

# Function to restore a table from Avro format
def restore_table(table_name, conn, schema):
    # Parse the schema
    parsed_schema = avro.schema.parse(json.dumps(schema))

    with open(f"{table_name}.avro", "rb") as file:
        datum_reader = avro.io.DatumReader(parsed_schema)
        data_file_reader = avro.datafile.DataFileReader(file, datum_reader)
        
        cursor = conn.cursor()
        for record in data_file_reader:
            columns = ', '.join(record.keys())
            placeholders = ', '.join(['%s'] * len(record))
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            # Execute the insert statement
            cursor.execute(sql, list(record.values()))
        
        data_file_reader.close()
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

# Restore the tables from Avro format
restore_table("hired_employees", conn, hired_employees_schema)
restore_table("departments", conn, departments_schema)
restore_table("jobs", conn, jobs_schema)

# Commit the changes
conn.commit()

# Close the database connection
conn.close()