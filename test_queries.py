import requests

base_url = 'http://localhost:5000/api/'

auth = ('admin', 'adminpass')

# Test the employees-hired-quarterly endpoint
def test_employees_hired_quarterly():
    url = base_url + 'employees-hired-quarterly'
    response = requests.get(url, auth=auth)
    if response.status_code == 200:
        data = response.json()
        print("Employees hired quarterly:")
        for item in data:
            print(item)
    else:
        print(f"Failed to get employees hired quarterly. Status code: {response.status_code}")

# Test the departments-above-mean endpoint
def test_departments_above_mean():
    url = base_url + 'departments-above-mean'
    response = requests.get(url, auth=auth)
    if response.status_code == 200:
        data = response.json()
        print("\nDepartments hiring above mean:")
        for item in data:
            print(item)
    else:
        print(f"Failed to get departments above mean. Status code: {response.status_code}")

if __name__ == '__main__':
    test_employees_hired_quarterly()
    test_departments_above_mean()