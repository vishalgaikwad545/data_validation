"""
Supervisor agent that coordinates the validation workflow
"""
from typing import Dict, List, Any, TypedDict
import os
from langchain_groq import ChatGroq
from langchain.schema import HumanMessage, SystemMessage
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from pydantic import BaseModel, Field

import config
from utils.data_loader import load_excel, load_parquets_to_sqlite, get_db_schema
from agents.code_validator import CodeValidator
from agents.zipcode_validator import ZipcodeValidator
from agents.reporting import ReportingAgent

# Define the state for the graph
class ValidationState(TypedDict):
    excel_data: Dict[str, Any]
    parquet_tables: List[str]
    db_schema: Dict[str, List[str]]
    db_path: str
    status: str
    code_validation_results: List[Dict[str, Any]]
    zipcode_validation_results: List[Dict[str, Any]]
    final_report: List[Dict[str, Any]]

def create_validation_graph():
    """
    Create the validation workflow graph
    """
    # Initialize the LLM
    llm = ChatGroq(
        api_key=config.GROQ_API_KEY,
        model=config.LLM_MODEL
    )
    
    # Create the validation agents
    code_validator = CodeValidator(llm=llm)
    zipcode_validator = ZipcodeValidator(llm=llm)
    reporting_agent = ReportingAgent(llm=llm)
    
    # Create the graph
    workflow = StateGraph(ValidationState)
    
    # Add nodes to the graph
    workflow.add_node("code_validation", code_validator.validate)
    workflow.add_node("zipcode_validation", zipcode_validator.validate)
    workflow.add_node("generate_report", reporting_agent.generate_report)
    
    # Define the edges
    workflow.add_edge("code_validation", "zipcode_validation")
    workflow.add_edge("zipcode_validation", "generate_report")
    workflow.add_edge("generate_report", END)
    
    # Compile the graph
    return workflow.compile()

def initialize_validation(excel_file_path: str, parquet_file_paths: List[str]) -> ValidationState:
    """
    Initialize the validation workflow
    """
    # Load the Excel data
    excel_data = load_excel(excel_file_path)
    
    # Load the Parquet files into SQLite
    parquet_tables = load_parquets_to_sqlite(parquet_file_paths, config.DB_PATH)
    
    # Get the database schema
    db_schema = get_db_schema(config.DB_PATH)
    
    # Initialize the validation state
    return ValidationState(
        excel_data=excel_data,
        parquet_tables=parquet_tables,
        db_schema=db_schema,
        db_path=config.DB_PATH,
        status="initialized",
        code_validation_results=[],
        zipcode_validation_results=[],
        final_report=[]
    )

def run_validation_workflow(excel_file_path: str, parquet_file_paths: List[str]) -> ValidationState:
    """
    Run the validation workflow
    """
    # Initialize the validation state
    state = initialize_validation(excel_file_path, parquet_file_paths)
    
    # Create the validation graph
    validation_graph = create_validation_graph()
    
    # Execute the graph
    result = validation_graph.invoke(state)
    
    return result
