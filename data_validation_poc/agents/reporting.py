"""
Reporting agent for data validation workflow
"""
import pandas as pd
from typing import Dict, List, Any

class ReportingAgent:
    """
    Agent responsible for generating validation reports
    """
    
    def generate_report(self, code_results: List[Dict[str, Any]], zipcode_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generate a comprehensive report from validation results
        
        Args:
            code_results: Results from code validation
            zipcode_results: Results from zipcode validation
            
        Returns:
            Comprehensive report as a list of dictionaries
        """
        report = []
        
        # Process code validation results
        for result in code_results:
            for value in result['matching_values']:
                report_item = {
                    "source": "code_validation",
                    "table": result['table'],
                    "column": result['column'],
                    "issue_type": "Invalid Code",
                    "value": value,
                    "recommendation": f"Update to valid code or remove"
                }
                report.append(report_item)
                
        # Process zipcode validation results
        for result in zipcode_results:
            report_item = {
                "source": "zipcode_validation",
                "table": result['table'],
                "column": result['column'],
                "issue_type": "Small Zipcode",
                "value": result['small_zip'],
                "recommendation": f"Replaced original ZIP code {result['small_zip']} with neighboring one ({result['reporting_zip']}) due to low population density."
            }
            report.append(report_item)
            
        return report
        
    def generate_summary(self, code_results: List[Dict[str, Any]], zipcode_results: List[Dict[str, Any]]) -> str:
        """
        Generate a text summary of the validation results
        
        Args:
            code_results: Results from code validation
            zipcode_results: Results from zipcode validation
            
        Returns:
            Summary text
        """
        # Count unique tables and columns with issues
        code_tables = set()
        code_columns = set()
        for result in code_results:
            code_tables.add(result['table'])
            code_columns.add(f"{result['table']}.{result['column']}")
            
        zipcode_tables = set()
        zipcode_columns = set()
        for result in zipcode_results:
            zipcode_tables.add(result['table'])
            zipcode_columns.add(f"{result['table']}.{result['column']}")
            
        # Generate summary text
        summary = []
        
        # Code validation summary
        code_count = len(code_results)
        if code_count > 0:
            summary.append(f"Found {code_count} code validation issues across {len(code_tables)} tables and {len(code_columns)} columns.")
        else:
            summary.append("No code validation issues found.")
            
        # Zipcode validation summary
        zipcode_count = len(zipcode_results)
        if zipcode_count > 0:
            summary.append(f"Found {zipcode_count} zipcode validation issues across {len(zipcode_tables)} tables and {len(zipcode_columns)} columns.")
        else:
            summary.append("No zipcode validation issues found.")
            
        # Overall summary
        total_issues = code_count + zipcode_count
        if total_issues > 0:
            summary.append(f"\nTotal: {total_issues} issues requiring attention.")
        else:
            summary.append("\nAll data passed validation checks.")
            
        return "\n".join(summary)
    
    @staticmethod
    def generate_csv_report(report_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Convert report data to DataFrame for CSV export
        
        Args:
            report_data: Report data as list of dictionaries
            
        Returns:
            DataFrame ready for CSV export
        """
        if not report_data:
            return pd.DataFrame()
            
        return pd.DataFrame(report_data)
