import requests


base_url = 'http://localhost:5000'

# authentication credentials
auth = ('admin', 'adminpass')

# Sample data 
insert_data = {
    'table': 'hired_employees',
    'records': [
        {'id': 1, 'name': 'Dan', 'datetime': '2024-06-15T12:00:00Z', 'department_id': 1, 'job_id': 1}
    ]
}

# Insert data for hired_employees table
response = requests.post(f'{base_url}/api/insert', json=insert_data, auth=auth)
print(f'Insertion Response (hired_employees): {response.json()}')

# Sample data for departments table
insert_data = {
    'table': 'departments',
    'records': [
        {'id': 1, 'department': 'Engineering'},
        {'id': 2, 'department': 'Marketing'}
    ]
}

# Insert data for departments table
response = requests.post(f'{base_url}/api/insert', json=insert_data, auth=auth)
print(f'Insertion Response (departments): {response.json()}')

# Sample data for jobs table
insert_data = {
    'table': 'jobs',
    'records': [
        {'id': 1, 'job': 'Software Engineer'},
        {'id': 2, 'job': 'Marketing Specialist'}
    ]
}

# Insert data for jobs table
response = requests.post(f'{base_url}/api/insert', json=insert_data, auth=auth)
print(f'Insertion Response (jobs): {response.json()}')



