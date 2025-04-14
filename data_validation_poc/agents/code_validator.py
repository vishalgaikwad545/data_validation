"""
Agent for validating codes based on the codes sheet from Excel
"""
from typing import Dict, List, Any, Callable
import pandas as pd
from langchain.chains import LLMChain
from langchain.schema import HumanMessage, SystemMessage
from langchain.prompts import PromptTemplate
from langchain.tools import BaseTool
from pydantic import BaseModel, Field

import config
from utils.sql_helpers import check_column_exists, get_matching_rows

class CodeValidator:
    """
    Agent that validates codes based on the codes sheet from Excel
    """
    def __init__(self, llm):
        """
        Initialize the code validator agent
        """
        self.llm = llm
        
        # Define the system prompt for the SQL generation
        self.system_prompt = """
        You are an expert SQL analyst helping with data validation.
        You need to create SQL queries to find matching records between Excel data and the SQLite database.
        
        The Excel 'codes' sheet contains codes and IDs that need to be validated against the database.
        Your goal is to generate SQL queries to find records in the database tables that match the values in the Excel sheet.
        
        Only generate the SQL query without any additional explanations.
        """
        
        # Create a prompt template for generating SQL queries
        self.sql_template = PromptTemplate(
            input_variables=["table_name", "column_name", "excel_values"],
            template="""
            I need to find records in the SQLite database table '{table_name}' where the column '{column_name}' 
            matches any of the following values from the Excel sheet:
            
            {excel_values}
            
            Please generate a SQL query to retrieve these matching records.
            """
        )
        
        # Create an LLM chain for generating SQL queries
        self.sql_chain = LLMChain(
            llm=self.llm,
            prompt=self.sql_template
        )
    
    def validate(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate codes based on the codes sheet from Excel
        """
        # Get the codes sheet from Excel
        codes_df = state["excel_data"].get("codes")
        
        # Check if codes sheet exists
        if codes_df is None:
            state["status"] = "error"
            state["code_validation_results"] = [{
                "error": "No 'codes' sheet found in the Excel file."
            }]
            return state
        
        validation_results = []
        
        # For each column in the codes sheet
        for column in codes_df.columns:
            # Get unique values from the column
            values = codes_df[column].dropna().unique().tolist()
            
            # Check if any Parquet table has a column with the same name
            matching_tables = []
            for table_name in state["parquet_tables"]:
                if column.lower() in [col.lower() for col in state["db_schema"][table_name]]:
                    matching_tables.append(table_name)
            
            if matching_tables:
                # For each matching table, validate the values
                for table_name in matching_tables:
                    # Get matching rows
                    matching_df = get_matching_rows(
                        state["db_path"], 
                        table_name, 
                        column, 
                        values
                    )
                    
                    # Add to results
                    if not matching_df.empty:
                        matching_values = matching_df[column].tolist()
                        result = {
                            "column": column,
                            "table": table_name,
                            "matching_values": matching_values,
                            "matching_records": matching_df.to_dict('records'),
                            "validation_type": "code"
                        }
                        validation_results.append(result)
        
        # Update the state
        state["code_validation_results"] = validation_results
        state["status"] = "code_validation_completed"
        
        return state
