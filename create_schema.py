from avro import schema
from avro import io
import json

# hired_employees schema
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

# departments schema
departments_schema = {
    "type": "record",
    "name": "departments",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "department", "type": "string"}
    ]
}

# jobs schema
jobs_schema = {
    "type": "record",
    "name": "jobs",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "job", "type": "string"}
    ]
}

# Write schemas to files
with open('hired_employees.avsc', 'w') as f:
    json.dump(hired_employees_schema, f)

with open('departments.avsc', 'w') as f:
    json.dump(departments_schema, f)

with open('jobs.avsc', 'w') as f:
    json.dump(jobs_schema, f)