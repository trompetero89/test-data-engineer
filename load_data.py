import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://newuser:password@localhost:5432/postgresdb')

employees_columns = ['id', 'name', 'datetime', 'department_id', 'job_id']
departments_columns = ['id', 'department']
jobs_columns = ['id', 'job']

employees = pd.read_csv('hired_employees.csv', names=employees_columns)
departments = pd.read_csv('departments.csv', names=departments_columns)
jobs = pd.read_csv('jobs.csv', names=jobs_columns)

# Write DataFrames to PostgreSQL tables
employees.to_sql('hired_employees', engine, if_exists='replace', index=False)
departments.to_sql('departments', engine, if_exists='replace', index=False)
jobs.to_sql('jobs', engine, if_exists='replace', index=False)