# PowerShell script to analyze CSV files in Pordata directory
$pordataPath = "2 Data Understanding/data/raw/Pordata"

# Get all CSV files
$csvFiles = Get-ChildItem -Path $pordataPath -Filter "*.csv"

# Create output file
$outputFile = "pordata_analysis.txt"
Clear-Content $outputFile

foreach ($file in $csvFiles) {
    Write-Host "Processing: $($file.Name)"
    Add-Content $outputFile "=== $($file.Name) ==="
    
    try {
        # Get file size
        $size = [math]::Round($file.Length / 1MB, 2)
        Add-Content $outputFile "Size: $size MB"
        
        # Read first line to get headers
        $headers = Get-Content -Path $file.FullName -TotalCount 1
        Add-Content $outputFile "Headers: $headers"
        
        # Get line count
        $lineCount = (Get-Content -Path $file.FullName).Length
        Add-Content $outputFile "Total lines: $lineCount"
        
        # Read sample data
        $sample = Get-Content -Path $file.FullName -TotalCount 5 | Select-Object -Skip 1
        Add-Content $outputFile "Sample data:"
        $sample | ForEach-Object { Add-Content $outputFile "  $_" }
    }
    catch {
        Add-Content $outputFile "Error processing file: $_"
    }
    
    Add-Content $outputFile ""
}

Write-Host "Analysis complete. Results saved to $outputFile"
