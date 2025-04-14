"""
Main Streamlit application for data validation
"""
import os
import tempfile
import streamlit as st
import pandas as pd
import time
from typing import List, Dict, Any

# Import local modules
import config
from agents.supervisor import run_validation_workflow
from agents.reporting import ReportingAgent
from utils.data_loader import load_excel, load_parquet

# Set up the Streamlit page
st.set_page_config(
    page_title="Data Validation POC",
    page_icon="✅",
    layout="wide"
)

# Page title and description
st.title("Data Validation POC")
st.write("""
This application validates data from Parquet files against rules defined in Excel.
Upload your files below to get started.
""")

# Create a sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["File Upload", "Validation Results", "About"])

# Initialize session state
if "validation_state" not in st.session_state:
    st.session_state.validation_state = None
if "report_df" not in st.session_state:
    st.session_state.report_df = None
if "excel_data" not in st.session_state:
    st.session_state.excel_data = None
if "parquet_file_paths" not in st.session_state:
    st.session_state.parquet_file_paths = []

# File Upload page
if page == "File Upload":
    st.header("File Upload")
    
    # Create two columns for file upload
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Upload Excel File")
        uploaded_excel = st.file_uploader("Upload Excel file with validation rules", type=["xlsx", "xls"])
        
        if uploaded_excel is not None:
            # Save the uploaded file to a temporary location
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
                tmp_file.write(uploaded_excel.getvalue())
                excel_file_path = tmp_file.name
            
            # Store the Excel file path in session state
            st.session_state.excel_file_path = excel_file_path
            
            # Load Excel data and preview
            try:
                excel_data = load_excel(excel_file_path)
                st.session_state.excel_data = excel_data
                
                # Show preview of Excel sheets
                st.success(f"Excel file loaded successfully with {len(excel_data)} sheets")
                
                # Create tabs for each sheet
                sheet_tabs = st.tabs(list(excel_data.keys()))
                
                for i, (sheet_name, sheet_data) in enumerate(excel_data.items()):
                    with sheet_tabs[i]:
                        st.write(f"Preview of '{sheet_name}' sheet:")
                        st.dataframe(sheet_data.head(5))
            
            except Exception as e:
                st.error(f"Error loading Excel file: {str(e)}")
    
    with col2:
        st.subheader("Upload Parquet Files")
        uploaded_parquets = st.file_uploader("Upload Parquet files for validation", type=["parquet"], accept_multiple_files=True)
        
        if uploaded_parquets:
            # Save the uploaded files to temporary locations
            parquet_file_paths = []
            
            for uploaded_parquet in uploaded_parquets:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp_file:
                    tmp_file.write(uploaded_parquet.getvalue())
                    parquet_file_path = tmp_file.name
                    parquet_file_paths.append(parquet_file_path)
            
            # Store the Parquet file paths in session state
            st.session_state.parquet_file_paths = parquet_file_paths
            
            # Show preview of each Parquet file
            st.success(f"{len(parquet_file_paths)} Parquet files loaded successfully")
            
            # Create tabs for each Parquet file
            parquet_tabs = st.tabs([f"Parquet {i+1}" for i in range(len(parquet_file_paths))])
            
            for i, file_path in enumerate(parquet_file_paths):
                with parquet_tabs[i]:
                    try:
                        parquet_data = load_parquet(file_path)
                        st.write(f"Preview of Parquet file {i+1}:")
                        st.dataframe(parquet_data.head(5))
                        st.write(f"Columns: {', '.join(parquet_data.columns)}")
                        st.write(f"Rows: {len(parquet_data)}")
                    except Exception as e:
                        st.error(f"Error loading Parquet file {i+1}: {str(e)}")
    
    # Run validation button
    if st.session_state.excel_data and st.session_state.parquet_file_paths:
        if st.button("Run Validation"):
            with st.spinner("Running validation workflow..."):
                try:
                    # Run the validation workflow
                    st.session_state.validation_state = run_validation_workflow(
                        st.session_state.excel_file_path,
                        st.session_state.parquet_file_paths
                    )
                    
                    # Generate the report DataFrame using the static method
                    # This avoids the need to create a ReportingAgent instance
                    report_df = ReportingAgent.generate_csv_report(st.session_state.validation_state["final_report"])
                    st.session_state.report_df = report_df
                    
                    st.success("Validation completed! Go to 'Validation Results' to see the report.")
                    
                except Exception as e:
                    st.error(f"Error during validation: {str(e)}")
    else:
        st.info("Please upload both Excel and Parquet files to run validation.")

# Validation Results page
elif page == "Validation Results":
    st.header("Validation Results")
    
    if st.session_state.validation_state is None:
        st.info("No validation results yet. Please go to 'File Upload' to run validation.")
    else:
        # Display validation status
        status = st.session_state.validation_state["status"]
        if status == "report_generated":
            st.success("Validation completed successfully!")
        elif status == "error":
            st.error("Validation encountered errors. Please check the report for details.")
        else:
            st.warning(f"Validation status: {status}")
        
        # Display report summary
        if "report_summary" in st.session_state.validation_state:
            st.subheader("Summary")
            st.write(st.session_state.validation_state["report_summary"])
        
        # Show validation results
        st.subheader("Code Validation Results")
        code_results = st.session_state.validation_state.get("code_validation_results", [])
        
        if not code_results:
            st.info("No code validation matches found.")
        else:
            st.write(f"Found {len(code_results)} code validation matches.")
            
            for i, result in enumerate(code_results):
                with st.expander(f"Match {i+1}: {result['column']} in {result['table']}"):
                    st.write(f"Column: {result['column']}")
                    st.write(f"Table: {result['table']}")
                    st.write(f"Matching values: {', '.join(str(v) for v in result['matching_values'][:5])}{'...' if len(result['matching_values']) > 5 else ''}")
                    st.write("Sample matching records:")
                    st.dataframe(pd.DataFrame(result['matching_records'][:5]))
        
        st.subheader("Zipcode Validation Results")
        zipcode_results = st.session_state.validation_state.get("zipcode_validation_results", [])
        
        if not zipcode_results:
            st.info("No zipcode validation matches found.")
        else:
            st.write(f"Found {len(zipcode_results)} zipcode validation matches.")
            
            for i, result in enumerate(zipcode_results):
                with st.expander(f"Match {i+1}: {result['small_zip']} in {result['table']}"):
                    st.write(f"Table: {result['table']}")
                    st.write(f"Column: {result['column']}")
                    st.write(f"Small Zipcode: {result['small_zip']}")
                    st.write(f"Reporting Zipcode: {result['reporting_zip']}")
                    st.write(f"Recommendation: {result['recommendation']}")
                    st.write("Record:")
                    st.json(result['record'])
        
        # Show full report
        st.subheader("Full Report")
        if st.session_state.report_df is not None and not st.session_state.report_df.empty:
            st.dataframe(st.session_state.report_df)
            
            # Generate CSV for download
            csv = st.session_state.report_df.to_csv(index=False).encode('utf-8')
            
            st.download_button(
                label="Download Report as CSV",
                data=csv,
                file_name="validation_report.csv",
                mime="text/csv",
            )
        else:
            st.info("No report data available.")

# About page
elif page == "About":
    st.header("About")
    st.write("""
    ## Data Validation POC
    
    This application is a Proof of Concept (POC) for data validation using LangChain, LangGraph, and Streamlit.
    
    ### Features:
    
    1. **File Upload**:
       - Upload an Excel file with validation rules
       - Upload one or more Parquet files to validate
    
    2. **Validation Workflow**:
       - Code Validation: Validates codes from the Excel `codes` sheet against Parquet data
       - Zipcode Validation: Validates zipcodes against the Excel `zipcode` sheet
    
    3. **Reporting**:
       - Generates a downloadable CSV report
       - Shows detailed validation results
    
    ### Technologies Used:
    
    - **LangChain**: For building the validation agents and SQL operations
    - **LangGraph**: For orchestrating the validation workflow
    - **Streamlit**: For the user interface
    - **SQLite**: For loading and querying Parquet data
    - **Groq API**: For LLM-based operations
    
    ### How to Use:
    
    1. Upload an Excel file with a `codes` sheet and a `zipcode` sheet
    2. Upload one or more Parquet files to validate
    3. Run the validation workflow
    4. View and download the validation report
    """)

# Clean up temporary files when the app exits
def cleanup():
    """Clean up temporary files"""
    if hasattr(st.session_state, 'excel_file_path') and os.path.exists(st.session_state.excel_file_path):
        os.unlink(st.session_state.excel_file_path)
    
    for file_path in st.session_state.get('parquet_file_paths', []):
        if os.path.exists(file_path):
            os.unlink(file_path)

# Register the cleanup function
import atexit
atexit.register(cleanup)
