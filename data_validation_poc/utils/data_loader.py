"""
Functions to load data from Excel and Parquet files into SQLite database
"""
import os
import sqlite3
import pandas as pd
from typing import Dict, List, Tuple, Any
import uuid

def load_excel(file_path: str) -> Dict[str, pd.DataFrame]:
    """
    Load Excel file and return a dictionary of dataframes for each sheet
    """
    excel_data = pd.read_excel(file_path, sheet_name=None)
    return excel_data

def load_parquet(file_path: str) -> pd.DataFrame:
    """
    Load Parquet file and return a dataframe
    """
    return pd.read_parquet(file_path)

def load_parquets_to_sqlite(parquet_files: List[str], db_path: str) -> List[str]:
    """
    Load multiple Parquet files into SQLite database
    Returns a list of created table names
    """
    conn = sqlite3.connect(db_path)
    table_names = []
    
    for file_path in parquet_files:
        # Generate a unique table name based on file name
        base_name = os.path.basename(file_path).replace('.parquet', '')
        table_name = f"{base_name}_{uuid.uuid4().hex[:8]}"
        
        # Load parquet to dataframe
        df = load_parquet(file_path)
        
        # Write to SQLite
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        table_names.append(table_name)
    
    conn.close()
    return table_names

def get_db_schema(db_path: str) -> Dict[str, List[str]]:
    """
    Get schema information from SQLite database
    Returns a dictionary of table names and their columns
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    schema = {}
    for table in tables:
        table_name = table[0]
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        schema[table_name] = [col[1] for col in columns]  # col[1] is column name
    
    conn.close()
    return schema

def get_table_data(db_path: str, table_name: str, limit: int = 5) -> Tuple[List[str], List[List[Any]]]:
    """
    Get data from a specific table in the SQLite database
    Returns column names and rows
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns = [col[1] for col in cursor.fetchall()]
    
    cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit};")
    rows = cursor.fetchall()
    
    conn.close()
    return columns, rows