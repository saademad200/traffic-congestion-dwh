import pandas as pd
import os
from datetime import datetime
from pathlib import Path


def analyze_excel_file(file_path):
    """
    Analyzes an Excel file and returns information about each sheet including
    column names and their data types.
    
    Args:
        file_path (str): Path to the Excel file
        
    Returns:
        dict: Dictionary containing sheet names and their column information
    """
    try:
        # Verify file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Excel file not found at: {file_path}")
            
        # Read all sheets from Excel file
        excel_file = pd.ExcelFile(file_path)
        sheet_names = excel_file.sheet_names
        
        # Dictionary to store results
        sheets_info = {}
        
        # Analyze each sheet
        for sheet_name in sheet_names:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            
            # Get column information
            column_info = {
                'columns': list(df.columns),
                'dtypes': {col: str(df[col].dtype) for col in df.columns},
                'row_count': len(df)
            }
            
            sheets_info[sheet_name] = column_info
            
        return sheets_info
        
    except Exception as e:
        print(f"Error analyzing Excel file: {str(e)}")
        return None


def print_excel_analysis(file_path, output_dir=None):
    """
    Prints a formatted analysis of the Excel file and optionally writes to a file.
    
    Args:
        file_path (str): Path to the Excel file
        write_to_file (bool): If True, writes the analysis to a text file
    """
    analysis = analyze_excel_file(file_path)
    
    if analysis:
        # Prepare the output text
        output_lines = ["Excel File Analysis", "==================\n"]
        
        for sheet_name, info in analysis.items():
            output_lines.extend([
                f"Sheet Name: {sheet_name}",
                "-" * (len(sheet_name) + 12),
                f"Number of rows: {info['row_count']}",
                "\nColumns and Data Types:"
            ])
            
            for col in info['columns']:
                output_lines.append(f"- {col}: {info['dtypes'][col]}")
            output_lines.append("\n")
        
        # Join all lines with newline character
        output_text = "\n".join(output_lines)
        
        # Print to console
        print(output_text)
        
        # Write to file if flag is True
        if output_dir:
            # Create docs directory if it doesn't exist
            docs_dir = Path(output_dir)
            docs_dir.mkdir(exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = docs_dir / f"excel_analysis_{timestamp}.txt"
            
            # Write to file
            with open(output_file, 'w') as f:
                f.write(f"Analysis of: {file_path}\n")
                f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 50 + "\n\n")
                f.write(output_text)
            
            print(f"\nAnalysis has been written to: {output_file}")

if __name__ == "__main__":
    # Replace this with the path to your Excel file
    excel_path = input("Please enter the path to your Excel file: ")
    write_to_file = input("Write analysis to file? (y/n): ").lower() == 'y'
    print_excel_analysis(excel_path, write_to_file) 