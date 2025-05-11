# Configuration section
$inputStorageAccount = "<your-azure-storage-account>" # Replace with your actual Azure storage account name
$inputContainerName = "<your-azure-input-container>" # Replace with your actual input container name 
$inputConnectionString = "<your-azure-input-connection-string>" # Replace with your actual Azure connection string
$outputStorageAccount = "<your-azure-storage-account>" # Replace with your actual Azure storage account name
$outputContainerName = "<your-azure-output-container>" # Replace with your actual output container name
$outputConnectionString = "<your-azure-output-connection-string>" # Replace with your actual Azure connection string
$localFolderPath = "<your-local-folder-path>" # Replace with your actual local folder path
$outputFilePattern = "<your-output-file-pattern>" # Replace with your actual output file pattern

# Check if Azure CLI is installed
try {
    $azVersion = az --version
    Write-Host "Using Azure CLI: $azVersion"
}
catch {
    Write-Error "Azure CLI is not installed or not in PATH. Please install Azure CLI first."
    exit 1
}

# Find all MF4 files in the local folder structure
Write-Host "Finding MF4 files to upload..."
$mf4Files = Get-ChildItem -Path $localFolderPath -Filter "*.MF4" -Recurse
Write-Host "Found $($mf4Files.Count) MF4 files"

# Upload each MF4 file directly to the container root
Write-Host "Uploading MF4 files directly to '$inputContainerName' root..."
$startTime = Get-Date
$uploadCount = 0

foreach ($file in $mf4Files) {
    Write-Host "Uploading $($file.Name)..." -NoNewline
    
    try {
        # Upload directly to container root (no folder structure)
        az storage blob upload --file $file.FullName --container-name $inputContainerName --name $file.Name --connection-string $inputConnectionString
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host " Done!" -ForegroundColor Green
            $uploadCount++
        }
        else {
            Write-Host " Failed!" -ForegroundColor Red
        }
    }
    catch {
        Write-Host " Error: $($Error[0])" -ForegroundColor Red
    }
}

$uploadEndTime = Get-Date
$uploadDuration = ($uploadEndTime - $startTime).TotalSeconds
Write-Host "Upload of $uploadCount files completed in $uploadDuration seconds."

# Start monitoring for output files
Write-Host "Waiting for processed files to appear in output container '$outputContainerName'..."
$processingStartTime = Get-Date
$found = $false
$timeout = New-TimeSpan -Minutes 15  # 15 minute timeout
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

# Get baseline of existing files to avoid counting pre-existing files
$baselineFiles = @()
$baselineResult = az storage blob list --container-name $outputContainerName --connection-string $outputConnectionString --output json | ConvertFrom-Json
if ($baselineResult) {
    $baselineFiles = $baselineResult | Where-Object { $_.name -like "*$outputFilePattern*" } | ForEach-Object { $_.name }
}

Write-Host "Found $($baselineFiles.Count) existing files matching pattern. Will ignore these."

# Main monitoring loop
while (-not $found -and $stopwatch.Elapsed -lt $timeout) {
    # List blobs in output container
    $result = az storage blob list --container-name $outputContainerName --connection-string $outputConnectionString --output json | ConvertFrom-Json
    
    # Check for new files matching our pattern
    if ($result) {
        $currentFiles = $result | Where-Object { $_.name -like "*$outputFilePattern*" } | ForEach-Object { $_.name }
        
        # Find new files that weren't in the baseline
        $newFiles = @()
        foreach ($file in $currentFiles) {
            if ($baselineFiles -notcontains $file) {
                $newFiles += $file
            }
        }
        
        if ($newFiles.Count -gt 0) {
            $found = $true
            $processingEndTime = Get-Date
            $processingDuration = ($processingEndTime - $processingStartTime).TotalSeconds
            Write-Host "`nProcessing complete! New files found:" -ForegroundColor Green
            $newFiles | ForEach-Object { Write-Host "  $_" }
        }
    }
    
    if (-not $found) {
        Write-Host "." -NoNewline
        Start-Sleep -Seconds 1
    }
}

$stopwatch.Stop()

if ($found) {
    # Generate report
    Write-Host "`n============= Performance Report =============" -ForegroundColor Cyan
    Write-Host "Upload time: $uploadDuration seconds"
    Write-Host "Processing time: $processingDuration seconds"
    Write-Host "Total time: $($uploadDuration + $processingDuration) seconds"
    
    # List the new files
    Write-Host "`nNew output files found:" -ForegroundColor Cyan
    $newFiles | ForEach-Object { Write-Host "  $_" }
}
else {
    Write-Host "`nTimed out waiting for output files after $($timeout.TotalMinutes) minutes." -ForegroundColor Red
    Write-Host "Please verify that your Azure Function is properly configured and running." -ForegroundColor Yellow
}
