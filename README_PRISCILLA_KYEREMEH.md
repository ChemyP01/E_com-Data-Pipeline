# Ecommerce Pipeline

## Overview
The Ecommerce Pipeline is an ETL (Extract, Transform, Load) system designed to process raw data from CSV files and load it into a PostgreSQL database. The pipeline consists of Python scripts for data extraction and transformation, as well as a main script to orchestrate the ETL process.

## Project Structure
- **RawData/**: Contains raw CSV files to be processed.
- **RefineData/**: Destination folder for transformed data files.
- **market1transform.py**: Python script for extracting and transforming Market 1 data.
- **market2transform.py**: Python script for extracting and transforming Market 2 data.
- **etl_och.py**: Main Python script to execute the ETL process.
- **docker-compose.yml**: Docker Compose file for setting up PostgreSQL container.

## Setup
1. Ensure Python3, Docker, and Docker Compose are installed on your system.
2. Clone this repository to your local machine.
3. Place raw CSV files in the `RawData/` folder.git
4. Update the connection details in `etl_och.py` to match your PostgreSQL database.
5. Run the following command to start the PostgreSQL database: `docker-compose up -d`.
6. Install the required dependencies by running: `pip install -r requirements.txt`.

## Usage
1. Execute the ETL process by running: `python3 etl.py`.
2. Transformed data will be loaded into the PostgreSQL database.

## Additional Notes
- Data transformation scripts (`market1transform.py` and `market2transform.py`) may generate DtypeWarnings due to mixed data types in the CSV files. These warnings can be safely ignored unless they indicate potential data quality issues.
- Ensure the PostgreSQL server is running and accessible before executing the ETL process.

## Detailed ETL Pipeline Process
1. **Data Extraction (Extract):**
   - The extraction phase involves retrieving data from the `RawData` folder, containing multiple CSV files.
   - Two Python scripts (`market1transform.py` and `market2transform.py`) extract data from the CSV files in the `RawData` folder.
   - Each script processes three CSV files, corresponding to deliveries in Market 1 and Market 2, respectively.

2. **Data Transformation (Transform):**
   - Following data extraction, the transformation phase prepares the extracted data for loading into the database.
   - Transformations include addressing mixed data types and restructuring the data to adhere to the desired database schema.
   - Tasks such as column renaming and data type conversion are performed to ensure consistency and compatibility with the database.

3. **Data Loading (Load):**
   - Once transformed, the data is loaded into a PostgreSQL database.
   - The main Python script `etl_och.py` orchestrates the loading process, establishing a connection to the database using provided connection details.
   - Dynamic table creation is employed, with tables named `firstmarket` and `secondmarket` created based on the transformed data schema.
   - The transformed data is inserted into these tables using SQLAlchemy's `to_sql()` method, allowing for efficient loading.

4. **Error Handling and Logging:**
   - Robust error handling mechanisms are implemented to manage exceptions that may occur during the ETL process, such as syntax errors or connection failures.
   - Detailed logging statements are included throughout the code to provide visibility into the execution flow and assist with troubleshooting.
   - Standard error messages are displayed to notify users of any encountered issues, ensuring transparency and facilitating prompt resolution.

Overall, the ETL pipeline effectively manages the migration of raw CSV data from the `RawData` folder into a PostgreSQL database, employing a structured workflow encompassing extraction, transformation, and loading stages. This facilitates data integration and prepares the data for subsequent analysis and reporting.

## Dependencies
- pandas
- psycopg2
- SQLAlchemy

## License
This project is licensed under the [MIT License](LICENSE).S