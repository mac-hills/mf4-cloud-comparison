# Configuration section
$awsProfileName = "<your-aws-profile-name>" # Replace with your actual AWS profile name
$inputBucketName = "<your-aws-intput-bucket>" # Replace with your actual input bucket name
$outputBucketName = "<your-aws-output-bucket>" # Replace with your actual output bucket name
$folderPath = "<your-local-folder-path>" # Replace with your actual local folder path
$subfolderName = "<your-subfolder-name>" # Replace with your actual subfolder name
$inputFolderID = "<your-input-folder-id>" # Replace with your actual input folder ID

# Upload files and start timing
Write-Host "Uploading folder structure to S3..."
$uploadStartTime = Get-Date

# Use AWS S3 sync command to upload the entire folder structure
aws s3 sync $folderPath "s3://$inputBucketName/$inputFolderID" --profile $awsProfileName
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to upload folder to S3. Check your credentials and bucket name."
    exit 1
}

$uploadEndTime = Get-Date
$uploadDuration = ($uploadEndTime - $uploadStartTime).TotalSeconds
Write-Host "Upload completed in $uploadDuration seconds."

# Get baseline of existing files to avoid counting pre-existing files
Write-Host "Checking for existing parquet files to establish baseline..."
$baselineFiles = @()
$baselineResult = aws s3 ls "s3://$outputBucketName/" --recursive --profile $awsProfileName
if ($baselineResult) {
    $baselineFiles = $baselineResult | Select-String "$subfolderName.*\.parquet$"
}
Write-Host "Found $($baselineFiles.Count) existing files matching pattern. Will ignore these."

# Start processing time measurement
$processingStartTime = Get-Date
Write-Host "Waiting for processed files..."
$found = $false
$timeout = New-TimeSpan -Minutes 15  # 15 minute timeout
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

# Main monitoring loop
while (-not $found -and $stopwatch.Elapsed -lt $timeout) {
    # List all files in the output bucket
    $currentFiles = aws s3 ls "s3://$outputBucketName/" --recursive --profile $awsProfileName
    
    # Filter for files matching our pattern
    $matchingFiles = $currentFiles | Select-String "$subfolderName.*\.parquet$"
    
    # Find new files that weren't in the baseline
    $newFiles = @()
    foreach ($file in $matchingFiles) {
        $fileString = $file.ToString().Trim()
        $isNew = $true
        
        foreach ($baseline in $baselineFiles) {
            if ($baseline.ToString().Trim() -eq $fileString) {
                $isNew = $false
                break
            }
        }
        
        if ($isNew) {
            $newFiles += $fileString
        }
    }
    
    if ($newFiles.Count -gt 0) {
        $found = $true
        $processingEndTime = Get-Date
        $processingDuration = ($processingEndTime - $processingStartTime).TotalSeconds
        Write-Host "`nProcessing complete! New files found." -ForegroundColor Green
    }
    else {
        Write-Host "." -NoNewline
        Start-Sleep -Seconds 1
    }
}

$stopwatch.Stop()

if ($found) {
    # Extract just the file paths from the full AWS CLI output lines
    $cleanFiles = @()
    foreach ($file in $newFiles) {
        # Extract just the file path from the AWS CLI output line
        if ($file -match "\S+\s+\S+\s+\d+\s+(.+)") {
            $cleanFiles += $matches[1]
        }
        else {
            $cleanFiles += $file  # Fallback if regex doesn't match
        }
    }
    
    # Generate report
    Write-Host "`n============= Performance Report =============" -ForegroundColor Cyan
    Write-Host "Upload time: $uploadDuration seconds"
    Write-Host "Processing time: $processingDuration seconds"
    Write-Host "Total time: $($uploadDuration + $processingDuration) seconds"
    
    # List the new files
    Write-Host "`nNew output files found:" -ForegroundColor Cyan
    foreach ($file in $cleanFiles) {
        Write-Host "  $file"
    }
}
else {
    Write-Host "`nTimed out waiting for output files after $($timeout.TotalMinutes) minutes." -ForegroundColor Red
    Write-Host "Please verify that your input files were uploaded correctly and that the conversion process started." -ForegroundColor Yellow
}
