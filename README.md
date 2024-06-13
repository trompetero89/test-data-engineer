# test-data-engineer
# Project Setup

## 1. Install PostgreSQL using Homebrew:

```bash
brew install PostgreSQL
```
## 2. Create the database and set parameters (replace 'password' with your desired password):
```code
CREATE USER newuser WITH PASSWORD 'password';
CREATE DATABASE postgresdb;
GRANT ALL PRIVILEGES ON DATABASE postgresdb TO newuser;
```
## 3. Run the table_creation.sql file to create tables (replace 'username' and 'database_name' with your PostgreSQL username and database name):
```bash

psql -U username -d database_name
\i /path/table_creation.sql
```

## 4. Load data from CSV files using load_data.py script:
```bash
python load_data.py
```

## 5. Run create_schema.py to create schemas for Avro files:
```bash
python create_schema.py
```
## 6. Run the script containing the endpoints:
```bash
python app.py
```
## 7. Use test_data_load.py script to test data insertion using the API:
```bash
python test_data_load.py
```
## 8. Run avro_backup.py script to create backup files for every table:
```bash
python avro_backup.py
```
## 9. Run restore_table.py script to test the functionality of restoring tables from an Avro file:
```bash
python restore_table.py
```
