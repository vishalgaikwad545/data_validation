"""
Zipcode validation agent for data validation workflow
"""
import pandas as pd
from typing import Dict, List, Any
import sqlite3
import os

class ZipcodeValidationAgent:
    """
    Agent responsible for validating zipcodes in the data
    """
    
    def __init__(self, excel_data: Dict[str, pd.DataFrame]):
        """
        Initialize the Zipcode Validation Agent
        
        Args:
            excel_data: Dictionary of DataFrames from the Excel file
        """
        self.excel_data = excel_data
        self.zipcode_mappings = self._extract_zipcode_mappings()
        
    def _extract_zipcode_mappings(self) -> Dict[str, str]:
        """
        Extract zipcode mappings from the Excel data
        
        Returns:
            Dictionary mapping small zipcodes to reporting zipcodes
        """
        # Check if zipcode_mapping sheet exists
        if 'zipcode_mapping' in self.excel_data:
            zipcode_df = self.excel_data['zipcode_mapping']
            
            # Convert mapping to dictionary
            mapping = {}
            for _, row in zipcode_df.iterrows():
                if 'small_zip' in row and 'reporting_zip' in row:
                    # Convert to string and ensure proper formatting
                    small_zip = str(row['small_zip']).strip()
                    reporting_zip = str(row['reporting_zip']).strip()
                    
                    # Add to mapping
                    mapping[small_zip] = reporting_zip
                    
            return mapping
        return {}
        
    def validate_zipcodes(self, db_connection: sqlite3.Connection, table_name: str) -> List[Dict[str, Any]]:
        """
        Validates zipcodes in the data against reference zipcodes
        
        Args:
            db_connection: SQLite connection to the database
            table_name: Name of the table to validate
        
        Returns:
            List of validation results
        """
        results = []
        
        # Skip if no zipcode mappings
        if not self.zipcode_mappings:
            return results
            
        # Get table columns
        try:
            cursor = db_connection.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [col[1] for col in cursor.fetchall()]
            
            # Find zipcode-like columns (contains 'zip' in the name)
            zipcode_columns = [col for col in columns if 'zip' in col.lower()]
            
            # For each zipcode column
            for column in zipcode_columns:
                # Query for records with zipcodes in our mapping
                placeholders = ','.join(['?' for _ in self.zipcode_mappings.keys()])
                query = f"""
                SELECT * FROM {table_name} 
                WHERE {column} IN ({placeholders})
                LIMIT 10
                """
                
                cursor.execute(query, list(self.zipcode_mappings.keys()))
                records = cursor.fetchall()
                
                # Convert records to dictionaries
                column_names = [description[0] for description in cursor.description]
                records_dict = [dict(zip(column_names, record)) for record in records]
                
                # Add validation results for each matching record
                for record in records_dict:
                    if str(record[column]).strip() in self.zipcode_mappings:
                        small_zip = str(record[column]).strip()
                        reporting_zip = self.zipcode_mappings[small_zip]
                        
                        result = {
                            "table": table_name,
                            "column": column,
                            "small_zip": small_zip,
                            "reporting_zip": reporting_zip,
                            "recommendation": f"Replace {small_zip} with {reporting_zip}",
                            "record": record
                        }
                        results.append(result)
                        
        except Exception as e:
            print(f"Error in zipcode validation: {str(e)}")
            
        return results
