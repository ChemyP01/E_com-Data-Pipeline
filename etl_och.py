import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def run_transformation_scripts():
    print("Executing code files for extracting and transforming data...")
    os.system("python market1transform.py")
    os.system("python market2transform.py")
    print("Data transformation complete.")

def create_table_from_csv(filename, table_name, conn):
    # Read the CSV file to determine schema
    df = pd.read_csv(os.path.join('RefineData', filename), low_memory=False)

    # Use pandas and SQLAlchemy to load data
    engine = create_engine('postgresql://ecompipe:ecompipe_passwd@localhost:5432/ecompipe')
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

    # Close the SQLAlchemy engine
    engine.dispose()

    print(f"Data loaded successfully into {table_name}.")

def main():
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="marketintel",
        user="username",
        password="userpassword"
    )
    
    run_transformation_scripts()
    
    # Load the transformed data into PostgreSQL
    create_table_from_csv('market1_data.csv', 'first_market', conn)
    create_table_from_csv('market2_data.csv', 'second_market', conn)

    # Close the connection
    conn.close()
    print("ETL process completed successfully.")

if __name__ == "__main__":
    main()
