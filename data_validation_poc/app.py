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
This application validates data from a Parquet file against rules defined in the Excel file.
Upload your Parquet file below to get started.
""")

# Create a sidebar for navigation with only two options
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["File Upload", "Validation Results"])

# Fixed Excel file path
EXCEL_FILE_PATH = "data/validation_rules.xlsx"  # Change this to your actual fixed path

# Initialize session state
if "validation_state" not in st.session_state:
    st.session_state.validation_state = None
if "report_df" not in st.session_state:
    st.session_state.report_df = None
if "excel_data" not in st.session_state:
    st.session_state.excel_data = None
if "parquet_file_path" not in st.session_state:
    st.session_state.parquet_file_path = None

# File Upload page
if page == "File Upload":
    st.header("File Upload")
    
    # Load Excel data from fixed path without preview
    excel_file_path = EXCEL_FILE_PATH
    
    try:
        # Check if the Excel file exists
        if os.path.exists(excel_file_path):
            # Load Excel data without preview
            excel_data = load_excel(excel_file_path)
            st.session_state.excel_data = excel_data
            st.session_state.excel_file_path = excel_file_path
            
            # Just display a simple success message
            st.subheader("Excel File (Fixed Path)")
            st.success(f"Excel file loaded successfully from: {excel_file_path}")
        else:
            st.error(f"Excel file not found at: {excel_file_path}")
            st.session_state.excel_data = None
    except Exception as e:
        st.error(f"Error loading Excel file: {str(e)}")
        st.session_state.excel_data = None
    
    # Single Parquet file upload
    st.subheader("Upload Parquet File")
    uploaded_parquet = st.file_uploader("Upload Parquet file for validation", type=["parquet"])
    
    if uploaded_parquet:
        # Save the uploaded file to a temporary location
        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp_file:
            tmp_file.write(uploaded_parquet.getvalue())
            parquet_file_path = tmp_file.name
        
        # Store the Parquet file path in session state
        st.session_state.parquet_file_path = parquet_file_path
        
        # Show preview of the Parquet file
        st.success(f"Parquet file loaded successfully: {uploaded_parquet.name}")
        
        try:
            parquet_data = load_parquet(parquet_file_path)
            st.write(f"Preview of Parquet data:")
            st.dataframe(parquet_data.head(5))
            st.write(f"Columns: {', '.join(parquet_data.columns)}")
            st.write(f"Rows: {len(parquet_data)}")
        except Exception as e:
            st.error(f"Error loading Parquet file: {str(e)}")
    
    # Run validation button
    if st.session_state.excel_data and st.session_state.parquet_file_path:
        if st.button("Run Validation"):
            with st.spinner("Running validation workflow..."):
                try:
                    # Run the validation workflow with a list containing the single Parquet file path
                    st.session_state.validation_state = run_validation_workflow(
                        st.session_state.excel_file_path,
                        [st.session_state.parquet_file_path]
                    )
                    
                    # Generate the report DataFrame using the static method
                    report_df = ReportingAgent.generate_csv_report(st.session_state.validation_state["final_report"])
                    st.session_state.report_df = report_df
                    
                    st.success("Validation completed! Go to 'Validation Results' to see the report.")
                    
                except Exception as e:
                    st.error(f"Error during validation: {str(e)}")
    else:
        if not st.session_state.excel_data:
            st.warning("Excel file not loaded. Please check the file path.")
        if not st.session_state.parquet_file_path:
            st.info("Please upload a Parquet file to run validation.")

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

# Clean up temporary files when the app exits
def cleanup():
    """Clean up temporary files"""
    if hasattr(st.session_state, 'parquet_file_path') and os.path.exists(st.session_state.parquet_file_path):
        os.unlink(st.session_state.parquet_file_path)

# Register the cleanup function
import atexit
atexit.register(cleanup)
