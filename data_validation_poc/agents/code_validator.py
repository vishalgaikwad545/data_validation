"""
Code validation agent for data validation workflow
"""
import pandas as pd
from typing import Dict, List, Any
import sqlite3
import os

class CodeValidationAgent:
    """
    Agent responsible for validating codes in the data
    """
    
    def __init__(self, excel_data: Dict[str, pd.DataFrame]):
        """
        Initialize the Code Validation Agent
        
        Args:
            excel_data: Dictionary of DataFrames from the Excel file
        """
        self.excel_data = excel_data
        self.valid_codes = self._extract_valid_codes()
        
    def _extract_valid_codes(self) -> Dict[str, List[str]]:
        """
        Extract valid codes from the Excel data
        
        Returns:
            Dictionary mapping code types to lists of valid codes
        """
        # Check if code_reference sheet exists
        if 'code_reference' in self.excel_data:
            code_df = self.excel_data['code_reference']
            
            # Convert to dictionary of valid codes by type
            valid_codes = {}
            for _, row in code_df.iterrows():
                if 'code_type' in row and 'valid_code' in row:
                    code_type = str(row['code_type']).strip()
                    valid_code = str(row['valid_code']).strip()
                    
                    if code_type not in valid_codes:
                        valid_codes[code_type] = []
                    
                    valid_codes[code_type].append(valid_code)
                    
            return valid_codes
        return {}
        
    def validate_codes(self, db_connection: sqlite3.Connection, table_name: str) -> List[Dict[str, Any]]:
        """
        Validates codes in the data against reference codes
        
        Args:
            db_connection: SQLite connection to the database
            table_name: Name of the table to validate
        
        Returns:
            List of validation results
        """
        results = []
        
        # Skip if no valid codes to check against
        if not self.valid_codes:
            return results
            
        # Get table columns
        try:
            cursor = db_connection.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [col[1] for col in cursor.fetchall()]
            
            # For each code type
            for code_type, valid_codes in self.valid_codes.items():
                # Find columns that might contain this code type
                possible_columns = [col for col in columns if code_type.lower() in col.lower()]
                
                # For each possible column
                for column in possible_columns:
                    # Check for invalid codes
                    placeholders = ','.join(['?' for _ in valid_codes])
                    query = f"""
                    SELECT DISTINCT {column} 
                    FROM {table_name} 
                    WHERE {column} IS NOT NULL 
                    AND {column} NOT IN ({placeholders})
                    LIMIT 100
                    """
                    
                    cursor.execute(query, valid_codes)
                    invalid_codes = [str(row[0]) for row in cursor.fetchall()]
                    
                    if invalid_codes:
                        # Get sample records for each invalid code
                        matching_records = []
                        for invalid_code in invalid_codes[:5]:  # Limit to first 5 for performance
                            sample_query = f"""
                            SELECT * FROM {table_name} 
                            WHERE {column} = ?
                            LIMIT 2
                            """
                            
                            cursor.execute(sample_query, (invalid_code,))
                            column_names = [description[0] for description in cursor.description]
                            records = cursor.fetchall()
                            record_dicts = [dict(zip(column_names, record)) for record in records]
                            matching_records.extend(record_dicts)
                            
                        # Create validation result
                        result = {
                            "table": table_name,
                            "column": column,
                            "code_type": code_type,
                            "matching_values": invalid_codes,
                            "matching_records": matching_records,
                            "valid_values": valid_codes
                        }
                        results.append(result)
                        
        except Exception as e:
            print(f"Error in code validation: {str(e)}")
            
        return results
