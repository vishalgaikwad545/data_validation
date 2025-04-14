"""
Utility functions for loading data from various sources
"""
import pandas as pd
import sqlite3
from typing import Dict, Any
import os

def load_excel(file_path: str) -> Dict[str, pd.DataFrame]:
    """
    Load Excel file with multiple sheets
    
    Args:
        file_path: Path to the Excel file
        
    Returns:
        Dictionary mapping sheet names to DataFrames
    """
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Excel file not found: {file_path}")
    
    # Load all sheets from Excel file
    excel_data = {}
    xlsx = pd.ExcelFile(file_path)
    
    for sheet_name in xlsx.sheet_names:
        excel_data[sheet_name] = pd.read_excel(xlsx, sheet_name=sheet_name)
        
    return excel_data

def load_parquet(file_path: str) -> pd.DataFrame:
    """
    Load Parquet file into a DataFrame
    
    Args:
        file_path: Path to the Parquet file
        
    Returns:
        DataFrame containing Parquet data
    """
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Parquet file not found: {file_path}")
    
    # Load Parquet file
    return pd.read_parquet(file_path)

def create_sqlite_db(df: pd.DataFrame, connection: sqlite3.Connection, table_name: str) -> None:
    """
    Create a SQLite table from a DataFrame
    
    Args:
        df: DataFrame to load into SQLite
        connection: SQLite connection object
        table_name: Name of the table to create
        
    Returns:
        None
    """
    # Check if DataFrame is empty
    if df.empty:
        raise ValueError("DataFrame is empty")
    
    # Convert DataFrame to SQLite table
    df.to_sql(table_name, connection, if_exists='replace', index=False)
    
    # Optimize table with indexes for common queries
    cursor = connection.cursor()
    
    # Create indexes on columns that might be frequently queried
    # This is a heuristic approach - specific indexes depend on query patterns
    columns = df.columns.tolist()
    
    # Create indexes on columns that might contain codes or IDs
    id_like_columns = [col for col in columns if 'id' in col.lower() or 'code' in col.lower() or 'key' in col.lower()]
    for col in id_like_columns:
        try:
            # Create index with a unique name based on table and column
            index_name = f"idx_{table_name}_{col}".replace(" ", "_").lower()
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({col})")
        except Exception as e:
            print(f"Warning: Could not create index on {col}: {str(e)}")
    
    # Commit changes
    connection.commit()
