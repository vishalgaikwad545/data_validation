"""
SQL utility functions for the application
"""
import sqlite3
from typing import List, Dict, Any, Optional
import pandas as pd

def execute_sql_query(db_path: str, query: str) -> pd.DataFrame:
    """
    Execute a SQL query on the SQLite database and return results as DataFrame
    """
    conn = sqlite3.connect(db_path)
    result = pd.read_sql_query(query, conn)
    conn.close()
    return result

def check_column_exists(db_path: str, table_name: str, column_name: str) -> bool:
    """
    Check if a column exists in a table
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns = cursor.fetchall()
    
    conn.close()
    return any(col[1].lower() == column_name.lower() for col in columns)

def find_tables_with_column(db_path: str, column_name: str) -> List[str]:
    """
    Find all tables that have a specific column
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    matching_tables = []
    for table in tables:
        table_name = table[0]
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        
        if any(col[1].lower() == column_name.lower() for col in columns):
            matching_tables.append(table_name)
    
    conn.close()
    return matching_tables

def check_value_exists(db_path: str, table_name: str, column_name: str, value: Any) -> bool:
    """
    Check if a specific value exists in a column
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} = ?", (value,))
    count = cursor.fetchone()[0]
    
    conn.close()
    return count > 0

def get_matching_rows(
    db_path: str, 
    table_name: str, 
    column_name: str, 
    values: List[Any],
    limit: Optional[int] = None
) -> pd.DataFrame:
    """
    Get rows where the column value matches any in the given list
    """
    if not values:
        return pd.DataFrame()
    
    conn = sqlite3.connect(db_path)
    
    # Format values for SQL IN clause
    formatted_values = ', '.join(['?' for _ in values])
    query = f"SELECT * FROM {table_name} WHERE {column_name} IN ({formatted_values})"
    
    if limit:
        query += f" LIMIT {limit}"
    
    result = pd.read_sql_query(query, conn, params=values)
    conn.close()
    return result
