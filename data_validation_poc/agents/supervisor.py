"""
Supervisor agent that coordinates the validation workflow
"""
import os
import sqlite3
import pandas as pd
from typing import Dict, List, Any
import tempfile

# Import local modules
from agents.code_validator import CodeValidationAgent
from agents.zipcode_validator import ZipcodeValidationAgent
from agents.reporting import ReportingAgent
from utils.data_loader import load_excel, load_parquet, create_sqlite_db

def run_validation_workflow(excel_file_path: str, parquet_file_paths: List[str]) -> Dict[str, Any]:
    """
    Run the validation workflow
    
    Args:
        excel_file_path: Path to the Excel file with validation rules
        parquet_file_paths: List of paths to Parquet files to validate
    
    Returns:
        Dictionary containing validation results and status
    """
    try:
        # Load Excel data
        excel_data = load_excel(excel_file_path)
        
        # Set up SQLite database for Parquet data
        db_path = os.path.join(tempfile.gettempdir(), "validation_data.db")
        
        # Create connection
        connection = sqlite3.connect(db_path)
        
        # Create tables for each Parquet file
        table_info = {}
        for parquet_path in parquet_file_paths:
            # Extract filename without extension
            file_name = os.path.basename(parquet_path)
            table_name = os.path.splitext(file_name)[0]
            table_name = table_name.replace("-", "_").replace(" ", "_").lower()
            
            # Load Parquet to SQLite
            parquet_data = load_parquet(parquet_path)
            create_sqlite_db(parquet_data, connection, table_name)
            
            # Store info
            table_info[table_name] = {
                "path": parquet_path,
                "name": table_name
            }
        
        # Initialize validators
        code_validator = CodeValidationAgent(excel_data)
        zipcode_validator = ZipcodeValidationAgent(excel_data)
        
        # Run validators on each table
        code_validation_results = []
        zipcode_validation_results = []
        
        for table_name in table_info.keys():
            # Run code validation
            code_results = code_validator.validate_codes(connection, table_name)
            code_validation_results.extend(code_results)
            
            # Run zipcode validation
            zipcode_results = zipcode_validator.validate_zipcodes(connection, table_name)
            zipcode_validation_results.extend(zipcode_results)
        
        # Generate report
        reporting_agent = ReportingAgent()
        final_report = reporting_agent.generate_report(code_validation_results, zipcode_validation_results)
        report_summary = reporting_agent.generate_summary(code_validation_results, zipcode_validation_results)
        
        # Close connection
        connection.close()
        
        # Clean up the temporary SQLite file
        if os.path.exists(db_path):
            os.remove(db_path)
        
        # Return results
        return {
            "status": "report_generated",
            "code_validation_results": code_validation_results,
            "zipcode_validation_results": zipcode_validation_results,
            "final_report": final_report,
            "report_summary": report_summary
        }
        
    except Exception as e:
        # Handle errors
        return {
            "status": "error",
            "error_message": str(e)
        }
