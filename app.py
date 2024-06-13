import os
import re
import requests
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, exc
import pandas as pd
import avro.schema
import avro.io
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)

# Basic Auth Setup
auth = HTTPBasicAuth()
users = {
    "admin": generate_password_hash("adminpass"),
}

@auth.verify_password
def verify_password(username, password):
    if username in users and check_password_hash(users.get(username), password):
        return username

# Database connection
db_url = os.getenv('DATABASE_URL', 'postgresql://newuser:password@localhost:5432/postgresdb')
engine = create_engine(db_url)

# Validation functions
def validate_hired_employees(records):
    for record in records:
        if not isinstance(record['id'], int):
            return "id must be an integer"
        if not isinstance(record['name'], str) or not record['name']:
            return "name must be a non-empty string"
        if not re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', record['datetime']):
            return "datetime must be in ISO format"
        if not isinstance(record['department_id'], int):
            return "department_id must be an integer"
        if not isinstance(record['job_id'], int):
            return "job_id must be an integer"
    return None

def validate_departments(records):
    for record in records:
        if not isinstance(record['id'], int):
            return "id must be an integer"
        if not isinstance(record['department'], str) or not record['department']:
            return "department must be a non-empty string"
    return None

def validate_jobs(records):
    for record in records:
        if not isinstance(record['id'], int):
            return "id must be an integer"
        if not isinstance(record['job'], str) or not record['job']:
            return "job must be a non-empty string"
    return None

validators = {
    'hired_employees': validate_hired_employees,
    'departments': validate_departments,
    'jobs': validate_jobs
}

# API Endpoints
@app.route('/api/insert', methods=['POST'])
@auth.login_required
def insert_data():
    try:
        data = request.get_json()
        table_name = data.get('table')
        records = data.get('records')

        if table_name not in validators:
            return jsonify({'status': 'error', 'message': 'Invalid table name'}), 400

        error_message = validators[table_name](records)
        if error_message:
            return jsonify({'status': 'error', 'message': error_message}), 400

        df = pd.DataFrame(records)
        df.to_sql(table_name, engine, if_exists='append', index=False)
        return jsonify({'status': 'success'}), 200
    except (exc.SQLAlchemyError, KeyError, TypeError, ValueError) as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/employees-hired-quarterly', methods=['GET'])
@auth.login_required
def employees_hired_quarterly():
    try:
        query = """
        SELECT 
            d.department, 
            j.job, 
            EXTRACT(QUARTER FROM he.datetime) AS quarter,
            COUNT(he.id) AS employees_hired
        FROM 
            hired_employees he
        JOIN 
            departments d ON he.department_id = d.id
        JOIN 
            jobs j ON he.job_id = j.id
        WHERE 
            EXTRACT(YEAR FROM he.datetime) = 2021
        GROUP BY 
            d.department, j.job, quarter
        ORDER BY 
            d.department, j.job, quarter;
        """
        df = pd.read_sql(query, engine)
        result = df.to_dict(orient='records')
        return jsonify(result), 200
    except exc.SQLAlchemyError as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

# Departments hiring above mean in 2021
@app.route('/api/departments-above-mean', methods=['GET'])
@auth.login_required
def departments_above_mean():
    try:
        query = """
        WITH department_hires AS (
            SELECT 
                d.id, 
                d.department, 
                COUNT(he.id) AS employees_hired
            FROM 
                hired_employees he
            JOIN 
                departments d ON he.department_id = d.id
            WHERE 
                EXTRACT(YEAR FROM he.datetime) = 2021
            GROUP BY 
                d.id, d.department
        ),
        mean_hires AS (
            SELECT 
                AVG(employees_hired) AS mean_employees_hired
            FROM 
                department_hires
        )
        SELECT 
            dh.id, 
            dh.department, 
            dh.employees_hired
        FROM 
            department_hires dh, mean_hires mh
        WHERE 
            dh.employees_hired > mh.mean_employees_hired
        ORDER BY 
            dh.employees_hired DESC;
        """
        df = pd.read_sql(query, engine)
        result = df.to_dict(orient='records')
        return jsonify(result), 200
    except exc.SQLAlchemyError as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500



if __name__ == '__main__':
    app.run(debug=True)