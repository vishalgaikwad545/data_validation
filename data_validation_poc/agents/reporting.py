"""
Agent for generating validation reports
"""
from typing import Dict, List, Any, Callable, Optional
import pandas as pd
from langchain.chains import LLMChain
from langchain.schema import HumanMessage, SystemMessage
from langchain.prompts import PromptTemplate
from langchain.tools import BaseTool
from pydantic import BaseModel, Field

import config

class ReportingAgent:
    """
    Agent that generates validation reports
    """
    def __init__(self, llm):
        """
        Initialize the reporting agent
        """
        self.llm = llm
        
        if llm is not None:
            # Define the system prompt for the report generation
            self.system_prompt = """
            You are an expert data analyst helping with generating validation reports.
            You need to combine the results from code validation and zipcode validation into a comprehensive report.
            The report should include all matching records, the source of the match, and recommendations.
            """
            
            # Create a prompt template for summarizing the report
            self.summary_template = PromptTemplate(
                input_variables=["code_results_count", "zipcode_results_count", "total_records"],
                template="""
                Please generate a summary of the validation report.
                
                Code Validation Results: {code_results_count} matches found
                Zipcode Validation Results: {zipcode_results_count} matches found
                Total Records in Report: {total_records}
                
                Please provide a concise summary of the validation results.
                """
            )
            
            # Create an LLM chain for generating summaries
            self.summary_chain = LLMChain(
                llm=llm,
                prompt=self.summary_template
            )
    
    def generate_report(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate a validation report
        """
        # Combine code and zipcode validation results
        code_results = state.get("code_validation_results", [])
        zipcode_results = state.get("zipcode_validation_results", [])
        
        # Initialize the report data
        report_data = []
        
        # Process code validation results
        for result in code_results:
            for record in result.get("matching_records", []):
                report_row = {
                    "source": "Code Validation",
                    "table": result["table"],
                    "column": result["column"],
                    "matching_value": record.get(result["column"]),
                    "recommendation": "",
                    **record
                }
                report_data.append(report_row)
        
        # Process zipcode validation results
        for result in zipcode_results:
            report_row = {
                "source": "Zipcode Validation",
                "table": result["table"],
                "column": result["column"],
                "matching_value": result["small_zip"],
                "recommendation": result["recommendation"],
                **result.get("record", {})
            }
            report_data.append(report_row)
        
        # Generate a summary of the report
        code_results_count = len(code_results)
        zipcode_results_count = len(zipcode_results)
        total_records = len(report_data)
        
        if hasattr(self, 'summary_chain'):
            summary_result = self.summary_chain.run(
                code_results_count=code_results_count,
                zipcode_results_count=zipcode_results_count,
                total_records=total_records
            )
        else:
            summary_result = f"Validation complete. Found {code_results_count} code matches and {zipcode_results_count} zipcode matches, for a total of {total_records} records."
        
        # Update the state
        state["final_report"] = report_data
        state["report_summary"] = summary_result
        state["status"] = "report_generated"
        
        return state
    
    @staticmethod
    def generate_csv_report(report_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Generate a CSV report from the validation results
        Static method that doesn't require an LLM
        """
        if not report_data:
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(report_data)
        
        # Ensure the important columns come first
        important_columns = ["source", "table", "column", "matching_value", "recommendation"]
        other_columns = [col for col in df.columns if col not in important_columns]
        
        # Reorder columns
        df = df[important_columns + other_columns]
        
        return df