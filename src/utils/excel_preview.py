import pandas as pd
import os


def preview_excel_data(file_path: str, max_rows: int = 5):
    """
    Preview data from all sheets in an Excel file
    
    Args:
        file_path: Path to the Excel file
        max_rows: Maximum number of rows to display per sheet
    """
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return
    
    print(f"\n=== Excel File Preview: {file_path} ===\n")
    
    # Load the Excel file
    xl = pd.ExcelFile(file_path)
    
    # Print sheet names
    sheet_names = xl.sheet_names
    print(f"Sheets in file: {sheet_names}\n")
    
    # Preview each sheet
    for sheet_name in sheet_names:
        print(f"=== Sheet: {sheet_name} ===")
        df = pd.read_excel(xl, sheet_name=sheet_name)
        
        # Print column names
        print(f"\nColumns: {list(df.columns)}")
        
        # Print sample data
        print(f"\nSample Data ({min(max_rows, len(df))} rows):")
        print(df.head(max_rows))
        
        # Print data types
        print("\nData Types:")
        print(df.dtypes)
        
        print("\n" + "="*50 + "\n")


if __name__ == "__main__":
    # Usage example
    excel_file_path = os.environ.get('EXCEL_FILE_PATH', 'data/traffic_flow_data.xlsx')
    preview_excel_data(excel_file_path) 