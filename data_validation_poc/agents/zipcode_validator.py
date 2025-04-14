"""
Agent for validating zipcodes based on the zipcode sheet from Excel
"""
from typing import Dict, List, Any, Callable
import pandas as pd
from langchain.chains import LLMChain
from langchain.schema import HumanMessage, SystemMessage
from langchain.prompts import PromptTemplate
from langchain.tools import BaseTool
from pydantic import BaseModel, Field

import config
from utils.sql_helpers import find_tables_with_column, get_matching_rows

class ZipcodeValidator:
    """
    Agent that validates zipcodes based on the zipcode sheet from Excel
    """
    def __init__(self, llm):
        """
        Initialize the zipcode validator agent
        """
        self.llm = llm
        
        # Define the system prompt for the zipcode validation
        self.system_prompt = """
        You are an expert data analyst helping with zipcode validation.
        You need to identify records with zipcodes that match the SMALL_ZIP_CODES from the Excel sheet.
        For each matching zipcode, you should recommend replacing it with the corresponding REPORTING_ZIP.
        """
        
        # Create a prompt template for generating recommendations
        self.recommendation_template = PromptTemplate(
            input_variables=["zipcode", "reporting_zip"],
            template="""
            The zipcode {zipcode} is in the SMALL_ZIP_CODES list. 
            I recommend replacing it with the reporting zipcode {reporting_zip}.
            
            Please provide a concise recommendation message.
            """
        )
        
        # Create an LLM chain for generating recommendations
        self.recommendation_chain = LLMChain(
            llm=self.llm,
            prompt=self.recommendation_template
        )
    
    def validate(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate zipcodes based on the zipcode sheet from Excel
        """
        # Get the zipcode sheet from Excel
        zipcode_df = state["excel_data"].get("zipcode")
        
        # Check if zipcode sheet exists
        if zipcode_df is None:
            state["status"] = "error"
            state["zipcode_validation_results"] = [{
                "error": "No 'zipcode' sheet found in the Excel file."
            }]
            return state
        
        # Check if the required columns exist in the zipcode sheet
        required_columns = ["SMALL_ZIP_CODES", "NEIGHBORS", "REPORTING_ZIP"]
        missing_columns = [col for col in required_columns if col not in zipcode_df.columns]
        
        if missing_columns:
            state["status"] = "error"
            state["zipcode_validation_results"] = [{
                "error": f"Missing columns in 'zipcode' sheet: {', '.join(missing_columns)}"
            }]
            return state
        
        # Get the small zip codes and reporting zip mapping
        small_zips = zipcode_df["SMALL_ZIP_CODES"].dropna().tolist()
        zipcode_mapping = dict(zip(zipcode_df["SMALL_ZIP_CODES"], zipcode_df["REPORTING_ZIP"]))
        
        # Find tables with zipcode column
        zipcode_columns = ["zipcode", "zip_code", "postal_code", "zip"]
        validation_results = []
        
        for table_name in state["parquet_tables"]:
            for zipcode_col in zipcode_columns:
                if zipcode_col.lower() in [col.lower() for col in state["db_schema"][table_name]]:
                    # Get matching rows
                    matching_df = get_matching_rows(
                        state["db_path"], 
                        table_name, 
                        zipcode_col, 
                        small_zips
                    )
                    
                    # Add to results
                    if not matching_df.empty:
                        for _, row in matching_df.iterrows():
                            small_zip = row[zipcode_col]
                            reporting_zip = zipcode_mapping.get(small_zip)
                            
                            if reporting_zip:
                                # Generate recommendation message
                                recommendation = f"Replace zipcode {small_zip} with reporting zipcode {reporting_zip}"
                                
                                result = {
                                    "column": zipcode_col,
                                    "table": table_name,
                                    "record": row.to_dict(),
                                    "small_zip": small_zip,
                                    "reporting_zip": reporting_zip,
                                    "recommendation": recommendation,
                                    "validation_type": "zipcode"
                                }
                                validation_results.append(result)
        
        # Update the state
        state["zipcode_validation_results"] = validation_results
        state["status"] = "zipcode_validation_completed"
        
        return state
