import os
import pandas as pd
from pathlib import Path

def analyze_csv_file(filepath):
    """Analyze a CSV file and return basic information"""
    try:
        # Read just the first few rows to understand structure
        df_sample = pd.read_csv(filepath, nrows=5)
        
        # Get file info
        file_stat = os.stat(filepath)
        size_mb = file_stat.st_size / (1024 * 1024)
        
        analysis = {
            'filename': os.path.basename(filepath),
            'size_mb': round(size_mb, 2),
            'shape': df_sample.shape,
            'columns': list(df_sample.columns),
            'dtypes': df_sample.dtypes.to_dict(),
            'sample_data': df_sample.head(3).to_dict('records'),
            'null_counts': df_sample.isnull().sum().to_dict()
        }
        return analysis
    except Exception as e:
        return {'filename': os.path.basename(filepath), 'error': str(e)}

def main():
    # Path to Pordata directory
    pordata_dir = Path("2 Data Understanding/data/raw/Pordata")
    
    # Find all CSV files
    csv_files = list(pordata_dir.glob("*.csv"))
    
    print(f"Found {len(csv_files)} CSV files in {pordata_dir}")
    print("\n" + "="*80)
    
    all_analysis = []
    
    for csv_file in csv_files:
        print(f"\nAnalyzing: {csv_file.name}")
        print("-"*40)
        
        analysis = analyze_csv_file(csv_file)
        all_analysis.append(analysis)
        
        if 'error' not in analysis:
            print(f"Size: {analysis['size_mb']} MB")
            print(f"Shape (sample): {analysis['shape']}")
            print(f"Columns: {analysis['columns']}")
            print(f"Data types: {analysis['dtypes']}")
            print(f"Null counts: {analysis['null_counts']}")
        else:
            print(f"Error: {analysis['error']}")
    
    # Save consolidated analysis
    import json
    with open('pordata_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(all_analysis, f, ensure_ascii=False, indent=2)
    
    print("\n" + "="*80)
    print("Analysis complete. Results saved to 'pordata_analysis.json'")

if __name__ == "__main__":
    main()
