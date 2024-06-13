DROP TABLE IF EXISTS hired_employees;
CREATE TABLE hired_employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    datetime TIMESTAMP,
    department_id INT,
    job_id INT
);
DROP TABLE IF EXISTS departments;
CREATE TABLE departments (
    id SERIAL PRIMARY KEY,
    department VARCHAR(100)
);
DROP TABLE IF EXISTS jobs;
CREATE TABLE jobs (
    id SERIAL PRIMARY KEY,
    job VARCHAR(100)
);
