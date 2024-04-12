import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def run_transformation_scripts():
    print("Executing code files for extracting and transforming data...")
    os.system("python m1_transformation.py")
    os.system("python m2_transformation.py")
    print("Data transformation complete.")

def create_table_from_csv(filename, table_name, conn):
    # Read the CSV file to determine schema
    df = pd.read_csv(os.path.join('transformed_data', filename), low_memory=False)

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
        database="ecompipe",
        user="ecompipe",
        password="ecompipe_passwd"
    )
    
    run_transformation_scripts()
    
    # Load the transformed data into PostgreSQL
    create_table_from_csv('market1_data.csv', 'market_one', conn)
    create_table_from_csv('market2_data.csv', 'market_two', conn)

    # Close the connection
    conn.close()
    print("ETL process completed successfully.")

if __name__ == "__main__":
    main()
