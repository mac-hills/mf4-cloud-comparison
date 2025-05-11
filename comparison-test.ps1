# Comprehensive script to compare AWS and Azure MF4 to Parquet conversion performance

#################################################################
# CONFIGURATION SECTION
#################################################################

# Global configuration
$localFolderPath = "<your-local-folder-path>"  # Replace with your actual local folder path
$subfolderName = "<your-subfolder-name>"  # Replace with your actual subfolder name
$inputFolderID = "<your-input-folder-id>"  # Replace with your actual input folder ID
$resultsFile = "comparison-results.json"
$testingOrder = "both"  # Options: "aws-first", "azure-first", "both"

# AWS Configuration
$awsProfileName = "<your-aws-profile-name>"  # Replace with your actual AWS profile name
$awsInputBucket = "<your-aws-input-bucket>"  # Replace with your actual input bucket name
$awsOutputBucket = "<your-aws-output-bucket>"  # Replace with your actual output bucket name
$awsRegion = "<your-aws-region>"  # Update to your AWS region

# Azure Configuration
$azureInputStorageAccount = "<your-azure-storage-account>"  # Replace with your actual Azure storage account name
$azureInputContainer = "<your-azure-input-container>"  # Replace with your actual input container name
$azureInputConnectionString = "<your-azure-input-connection-string>"  # Replace with your actual Azure connection string
$azureOutputStorageAccount = "<your-azure-storage-account>"  # Replace with your actual Azure storage account name
$azureOutputContainer = "<your-azure-output-container>"  # Replace with your actual output container name
$azureOutputConnectionString = "<your-azure-output-connection-string>"  # Replace with your actual Azure connection string

# Maximum wait time in seconds
$maxWaitTime = 900  # 15 minutes

#################################################################
# HELPER FUNCTIONS
#################################################################

function Get-FolderSize {
    param (
        [string]$Path
    )
    
    $size = (Get-ChildItem -Path $Path -Recurse -File | Measure-Object -Property Length -Sum).Sum
    return $size
}

function Format-FileSize {
    param (
        [long]$SizeInBytes
    )
    
    if ($SizeInBytes -gt 1GB) {
        return "$([math]::Round($SizeInBytes / 1GB, 2)) GB"
    }
    elseif ($SizeInBytes -gt 1MB) {
        return "$([math]::Round($SizeInBytes / 1MB, 2)) MB"
    }
    elseif ($SizeInBytes -gt 1KB) {
        return "$([math]::Round($SizeInBytes / 1KB, 2)) KB"
    }
    else {
        return "$SizeInBytes Bytes"
    }
}

function Format-Duration {
    param (
        [double]$Seconds
    )
    
    $ts = [timespan]::FromSeconds($Seconds)
    if ($ts.TotalHours -ge 1) {
        return "$($ts.Hours)h $($ts.Minutes)m $($ts.Seconds)s"
    }
    elseif ($ts.TotalMinutes -ge 1) {
        return "$($ts.Minutes)m $($ts.Seconds)s"
    }
    else {
        return "$($ts.Seconds).$($ts.Milliseconds.ToString('000'))s"
    }
}

function Write-SectionHeader {
    param (
        [string]$Title,
        [string]$Color = "Cyan"
    )
    
    Write-Host "`n$('=' * 60)" -ForegroundColor $Color
    Write-Host $Title -ForegroundColor $Color
    Write-Host "$('=' * 60)" -ForegroundColor $Color
}

function Check-PrerequisiteCommands {
    $prerequisites = @{
        "AWS CLI"   = "aws --version"
        "Azure CLI" = "az --version"
    }
    
    $allPresent = $true
    
    Write-SectionHeader "Checking Prerequisites"
    
    foreach ($prerequisite in $prerequisites.GetEnumerator()) {
        try {
            $result = Invoke-Expression $prerequisite.Value 2>&1
            Write-Host "$($prerequisite.Key): Installed" -ForegroundColor Green
        }
        catch {
            Write-Host "$($prerequisite.Key): Not found" -ForegroundColor Red
            $allPresent = $false
        }
    }
    
    if (-not $allPresent) {
        Write-Host "`nPlease install all required tools before continuing." -ForegroundColor Red
        exit 1
    }
    
    # Check Azure login status
    try {
        $azAccountInfo = az account show 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Please log in to Azure:" -ForegroundColor Yellow
            az login
        }
        else {
            Write-Host "Azure: Logged in" -ForegroundColor Green
        }
    }
    catch {
        Write-Host "Please log in to Azure:" -ForegroundColor Yellow
        az login
    }
}

#################################################################
# AWS TESTING FUNCTIONS
#################################################################

function Test-AWS {
    Write-SectionHeader "AWS MF4 TO PARQUET CONVERSION TEST" "Blue"
    
    # Get input metrics
    $inputSize = Get-FolderSize -Path $localFolderPath
    $inputFileCount = (Get-ChildItem -Path $localFolderPath -Recurse -Filter "*.MF4").Count
    Write-Host "Input data size: $(Format-FileSize -SizeInBytes $inputSize), $inputFileCount files"
    
    # Start CPU and memory counters
    try {
        $startCPU = (Get-Counter '\Processor(_Total)\% Processor Time').CounterSamples.CookedValue
        $startMemory = (Get-Counter '\Memory\Available MBytes').CounterSamples.CookedValue
    }
    catch {
        $startCPU = 0
        $startMemory = 0
        Write-Host "Unable to measure CPU/Memory. Continuing..." -ForegroundColor Yellow
    }
    
    # Upload files and start timing
    Write-Host "Uploading folder structure to AWS S3..."
    $uploadStartTime = Get-Date
    
    # Use AWS S3 sync command to upload the entire folder structure
    aws s3 sync $localFolderPath "s3://$awsInputBucket/$inputFolderID" --profile $awsProfileName
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to upload folder to S3. Check your credentials and bucket name."
        return $null
    }
    
    $uploadEndTime = Get-Date
    $uploadDuration = ($uploadEndTime - $uploadStartTime).TotalSeconds
    Write-Host "AWS upload completed in $(Format-Duration -Seconds $uploadDuration)"
    
    # Get baseline of existing files to avoid counting pre-existing files
    Write-Host "Checking for existing parquet files to establish baseline..."
    $baselineFiles = @()
    $baselineResult = aws s3 ls "s3://$awsOutputBucket/" --recursive --profile $awsProfileName
    if ($baselineResult) {
        $baselineFiles = $baselineResult | Select-String "$subfolderName.*\.parquet$"
    }
    Write-Host "Found $($baselineFiles.Count) existing files matching pattern. Will ignore these."
    
    # Start processing time measurement
    $processingStartTime = Get-Date
    Write-Host "Waiting for AWS processed files..."
    $found = $false
    $newFiles = @()
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    
    # Main monitoring loop
    while (-not $found -and $stopwatch.Elapsed.TotalSeconds -lt $maxWaitTime) {
        # List all files in the output bucket
        $currentFiles = aws s3 ls "s3://$awsOutputBucket/" --recursive --profile $awsProfileName
        
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
            Write-Host "`nAWS processing complete! Found $($newFiles.Count) new files." -ForegroundColor Green
            break
        }
        else {
            Write-Host "." -NoNewline
            Start-Sleep -Seconds 1
        }
    }
    
    $stopwatch.Stop()
    
    # Get final CPU and memory metrics
    try {
        $endCPU = (Get-Counter '\Processor(_Total)\% Processor Time').CounterSamples.CookedValue
        $endMemory = (Get-Counter '\Memory\Available MBytes').CounterSamples.CookedValue
    }
    catch {
        $endCPU = 0
        $endMemory = 0
    }
    
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
        
        # Get output sizes
        $outputSizes = @()
        $totalOutputSize = 0
        
        foreach ($file in $cleanFiles) {
            try {
                $fileInfoCmd = "aws s3api head-object --bucket $awsOutputBucket --key `"$file`" --profile $awsProfileName"
                $fileInfo = Invoke-Expression $fileInfoCmd | ConvertFrom-Json
                $size = $fileInfo.ContentLength
                $totalOutputSize += $size
                $outputSizes += @{
                    Path = $file
                    Size = $size
                }
            }
            catch {
                Write-Host "Error getting size for $file" -ForegroundColor Yellow
            }
        }
        
        # Calculate costs
        $s3StorageCostPerGB = 0.023 # S3 Standard storage cost per GB-month
        $s3PutCostPer1000 = 0.005 # PUT request cost per 1000
        $s3GetCostPer1000 = 0.0004 # GET request cost per 1000
        
        # Calculate storage costs (pro-rated for 1 day)
        $inputStorageCost = ($inputSize / 1GB) * ($s3StorageCostPerGB / 30)
        $outputStorageCost = ($totalOutputSize / 1GB) * ($s3StorageCostPerGB / 30)
        
        # Calculate request costs
        $putRequestCost = ($inputFileCount / 1000) * $s3PutCostPer1000
        $getRequestCost = ($cleanFiles.Count / 1000) * $s3GetCostPer1000
        
        $totalCost = $inputStorageCost + $outputStorageCost + $putRequestCost + $getRequestCost
        
        # Build the result object
        $awsResults = @{
            Platform             = "AWS"
            InputSize            = $inputSize
            InputFileCount       = $inputFileCount
            OutputSize           = $totalOutputSize
            OutputFileCount      = $cleanFiles.Count
            UploadDuration       = $uploadDuration
            ProcessingDuration   = $processingDuration
            TotalDuration        = ($uploadDuration + $processingDuration)
            CompressionRatio     = if ($inputSize -gt 0) { $totalOutputSize / $inputSize } else { 0 }
            InputThroughput      = if ($uploadDuration -gt 0) { $inputSize / 1MB / $uploadDuration } else { 0 }
            ProcessingThroughput = if ($processingDuration -gt 0) { $totalOutputSize / 1MB / $processingDuration } else { 0 }
            CPUUsage             = if ($startCPU -gt 0 -and $endCPU -gt 0) { ($startCPU + $endCPU) / 2 } else { 0 }
            MemoryChangeMB       = if ($startMemory -gt 0 -and $endMemory -gt 0) { $startMemory - $endMemory } else { 0 }
            EstimatedCost        = $totalCost
            StorageCost          = ($inputStorageCost + $outputStorageCost)
            RequestCost          = ($putRequestCost + $getRequestCost)
            OutputFiles          = $cleanFiles
        }
        
        return $awsResults
    }
    else {
        Write-Host "`nTimed out waiting for AWS output files after $($maxWaitTime / 60) minutes." -ForegroundColor Red
        return $null
    }
}

#################################################################
# AZURE TESTING FUNCTIONS
#################################################################

function Test-Azure {
    Write-SectionHeader "AZURE MF4 TO PARQUET CONVERSION TEST" "Magenta"
    
    # Get input metrics
    $inputSize = Get-FolderSize -Path $localFolderPath
    $inputFileCount = (Get-ChildItem -Path $localFolderPath -Recurse -Filter "*.MF4").Count
    Write-Host "Input data size: $(Format-FileSize -SizeInBytes $inputSize), $inputFileCount files"
    
    # Start CPU and memory counters
    try {
        $startCPU = (Get-Counter '\Processor(_Total)\% Processor Time').CounterSamples.CookedValue
        $startMemory = (Get-Counter '\Memory\Available MBytes').CounterSamples.CookedValue
    }
    catch {
        $startCPU = 0
        $startMemory = 0
        Write-Host "Unable to measure CPU/Memory. Continuing..." -ForegroundColor Yellow
    }
    
    # Find all MF4 files in the local folder structure
    Write-Host "Finding MF4 files to upload..."
    $mf4Files = Get-ChildItem -Path $localFolderPath -Filter "*.MF4" -Recurse
    Write-Host "Found $($mf4Files.Count) MF4 files"
    
    # Upload each MF4 file directly to the container root
    Write-Host "Uploading MF4 files directly to '$azureInputContainer' root..."
    $uploadStartTime = Get-Date
    $uploadCount = 0
    
    foreach ($file in $mf4Files) {
        Write-Host "Uploading $($file.Name)..." -NoNewline
        
        try {
            # Upload directly to container root (no folder structure)
            az storage blob upload --file $file.FullName --container-name $azureInputContainer --name $file.Name --connection-string $azureInputConnectionString
            
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
    $uploadDuration = ($uploadEndTime - $uploadStartTime).TotalSeconds
    Write-Host "Azure upload of $uploadCount files completed in $(Format-Duration -Seconds $uploadDuration)"
    
    # Get baseline of existing files to avoid counting pre-existing files
    $baselineFiles = @()
    $baselineResult = az storage blob list --container-name $azureOutputContainer --connection-string $azureOutputConnectionString --output json | ConvertFrom-Json
    if ($baselineResult) {
        $baselineFiles = $baselineResult | Where-Object { $_.name -like "*$subfolderName*" } | ForEach-Object { $_.name }
    }
    Write-Host "Found $($baselineFiles.Count) existing files matching pattern. Will ignore these."
    
    # Start monitoring for output files
    Write-Host "Waiting for processed files to appear in output container '$azureOutputContainer'..."
    $processingStartTime = Get-Date
    $found = $false
    $newFiles = @()
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    
    # Main monitoring loop
    while (-not $found -and $stopwatch.Elapsed.TotalSeconds -lt $maxWaitTime) {
        # List blobs in output container
        $result = az storage blob list --container-name $azureOutputContainer --connection-string $azureOutputConnectionString --output json | ConvertFrom-Json
        
        # Check for new files matching our pattern
        if ($result) {
            $currentFiles = $result | Where-Object { $_.name -like "*$subfolderName*" } | ForEach-Object { $_.name }
            
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
                Write-Host "`nAzure processing complete! Found $($newFiles.Count) new files." -ForegroundColor Green
                break
            }
        }
        
        if (-not $found) {
            Write-Host "." -NoNewline
            Start-Sleep -Seconds 1
        }
    }
    
    $stopwatch.Stop()
    
    # Get final CPU and memory metrics
    try {
        $endCPU = (Get-Counter '\Processor(_Total)\% Processor Time').CounterSamples.CookedValue
        $endMemory = (Get-Counter '\Memory\Available MBytes').CounterSamples.CookedValue
    }
    catch {
        $endCPU = 0
        $endMemory = 0
    }
    
    if ($found) {
        # Get output sizes
        $outputSizes = @()
        $totalOutputSize = 0
        
        foreach ($file in $newFiles) {
            try {
                $fileInfo = az storage blob show --container-name $azureOutputContainer --name $file --connection-string $azureOutputConnectionString --output json | ConvertFrom-Json
                $size = $fileInfo.properties.contentLength
                $totalOutputSize += $size
                $outputSizes += @{
                    Path = $file
                    Size = $size
                }
            }
            catch {
                Write-Host "Error getting size for $file" -ForegroundColor Yellow
            }
        }
        
        # Calculate costs
        $azureStorageCostPerGB = 0.0184 # Azure Storage (LRS) cost per GB-month
        $azurePutCostPer10000 = 0.05 # PUT operation cost per 10,000
        $azureGetCostPer10000 = 0.004 # GET operation cost per 10,000
        
        # Calculate storage costs (pro-rated for 1 day)
        $inputStorageCost = ($inputSize / 1GB) * ($azureStorageCostPerGB / 30)
        $outputStorageCost = ($totalOutputSize / 1GB) * ($azureStorageCostPerGB / 30)
        
        # Calculate operation costs
        $putOperationCost = ($inputFileCount / 10000) * $azurePutCostPer10000
        $getOperationCost = ($newFiles.Count / 10000) * $azureGetCostPer10000
        
        $totalCost = $inputStorageCost + $outputStorageCost + $putOperationCost + $getOperationCost
        
        # Build the result object
        $azureResults = @{
            Platform             = "Azure"
            InputSize            = $inputSize
            InputFileCount       = $inputFileCount
            OutputSize           = $totalOutputSize
            OutputFileCount      = $newFiles.Count
            UploadDuration       = $uploadDuration
            ProcessingDuration   = $processingDuration
            TotalDuration        = ($uploadDuration + $processingDuration)
            CompressionRatio     = if ($inputSize -gt 0) { $totalOutputSize / $inputSize } else { 0 }
            InputThroughput      = if ($uploadDuration -gt 0) { $inputSize / 1MB / $uploadDuration } else { 0 }
            ProcessingThroughput = if ($processingDuration -gt 0) { $totalOutputSize / 1MB / $processingDuration } else { 0 }
            CPUUsage             = if ($startCPU -gt 0 -and $endCPU -gt 0) { ($startCPU + $endCPU) / 2 } else { 0 }
            MemoryChangeMB       = if ($startMemory -gt 0 -and $endMemory -gt 0) { $startMemory - $endMemory } else { 0 }
            EstimatedCost        = $totalCost
            StorageCost          = ($inputStorageCost + $outputStorageCost)
            RequestCost          = ($putOperationCost + $getOperationCost)
            OutputFiles          = $newFiles
        }
        
        return $azureResults
    }
    else {
        Write-Host "`nTimed out waiting for Azure output files after $($maxWaitTime / 60) minutes." -ForegroundColor Red
        return $null
    }
}

#################################################################
# REPORTING FUNCTIONS
#################################################################

function Show-ComparisonReport {
    param (
        $AwsResults,
        $AzureResults
    )
    
    Write-SectionHeader "COMPARISON REPORT" "Yellow"
    
    # Check if we have results from both platforms
    if ($null -eq $AwsResults -and $null -eq $AzureResults) {
        Write-Host "No results available for comparison." -ForegroundColor Red
        return
    }
    
    # Create comparison table
    $comparisonTable = @(
        @{Category = "Timing Metrics (seconds)"; Metric = ""; AWS = ""; Azure = ""; Difference = ""; Winner = "" }
    )
    
    # Add timing metrics
    if ($AwsResults -and $AzureResults) {
        $comparisonTable += @{
            Category   = "Timing Metrics (seconds)"
            Metric     = "Upload Time"
            AWS        = [math]::Round($AwsResults.UploadDuration, 2)
            Azure      = [math]::Round($AzureResults.UploadDuration, 2)
            Difference = [math]::Round([math]::Abs($AwsResults.UploadDuration - $AzureResults.UploadDuration), 2)
            Winner     = if ($AwsResults.UploadDuration -lt $AzureResults.UploadDuration) { "AWS" } else { "Azure" }
        }
        
        $comparisonTable += @{
            Category   = "Timing Metrics (seconds)"
            Metric     = "Processing Time"
            AWS        = [math]::Round($AwsResults.ProcessingDuration, 2)
            Azure      = [math]::Round($AzureResults.ProcessingDuration, 2)
            Difference = [math]::Round([math]::Abs($AwsResults.ProcessingDuration - $AzureResults.ProcessingDuration), 2)
            Winner     = if ($AwsResults.ProcessingDuration -lt $AzureResults.ProcessingDuration) { "AWS" } else { "Azure" }
        }
        
        $comparisonTable += @{
            Category   = "Timing Metrics (seconds)"
            Metric     = "Total Time"
            AWS        = [math]::Round($AwsResults.TotalDuration, 2)
            Azure      = [math]::Round($AzureResults.TotalDuration, 2)
            Difference = [math]::Round([math]::Abs($AwsResults.TotalDuration - $AzureResults.TotalDuration), 2)
            Winner     = if ($AwsResults.TotalDuration -lt $AzureResults.TotalDuration) { "AWS" } else { "Azure" }
        }
    }
    
    # Add throughput metrics
    $comparisonTable += @{
        Category   = "Performance Metrics"; 
        Metric     = ""; 
        AWS        = ""; 
        Azure      = ""; 
        Difference = ""; 
        Winner     = ""
    }
    
    if ($AwsResults -and $AzureResults) {
        $comparisonTable += @{
            Category   = "Performance Metrics"
            Metric     = "Input Throughput (MB/s)"
            AWS        = [math]::Round($AwsResults.InputThroughput, 2)
            Azure      = [math]::Round($AzureResults.InputThroughput, 2)
            Difference = [math]::Round([math]::Abs($AwsResults.InputThroughput - $AzureResults.InputThroughput), 2)
            Winner     = if ($AwsResults.InputThroughput -gt $AzureResults.InputThroughput) { "AWS" } else { "Azure" }
        }
        
        $comparisonTable += @{
            Category   = "Performance Metrics"
            Metric     = "Processing Throughput (MB/s)"
            AWS        = [math]::Round($AwsResults.ProcessingThroughput, 2)
            Azure      = [math]::Round($AzureResults.ProcessingThroughput, 2)
            Difference = [math]::Round([math]::Abs($AwsResults.ProcessingThroughput - $AzureResults.ProcessingThroughput), 2)
            Winner     = if ($AwsResults.ProcessingThroughput -gt $AzureResults.ProcessingThroughput) { "AWS" } else { "Azure" }
        }
    }
    
    # Add size metrics
    $comparisonTable += @{
        Category   = "Size Metrics"; 
        Metric     = ""; 
        AWS        = ""; 
        Azure      = ""; 
        Difference = ""; 
        Winner     = ""
    }
    
    if ($AwsResults -and $AzureResults) {
        $comparisonTable += @{
            Category   = "Size Metrics"
            Metric     = "Output Size (MB)"
            AWS        = [math]::Round($AwsResults.OutputSize / 1MB, 2)
            Azure      = [math]::Round($AzureResults.OutputSize / 1MB, 2)
            Difference = [math]::Round([math]::Abs(($AwsResults.OutputSize - $AzureResults.OutputSize) / 1MB), 2)
            Winner     = "N/A"  # Size itself doesn't determine a winner
        }
        
        $comparisonTable += @{
            Category   = "Size Metrics"
            Metric     = "Compression Ratio"
            AWS        = [math]::Round($AwsResults.CompressionRatio, 4)
            Azure      = [math]::Round($AzureResults.CompressionRatio, 4)
            Difference = [math]::Round([math]::Abs($AwsResults.CompressionRatio - $AzureResults.CompressionRatio), 4)
            Winner     = if ($AwsResults.CompressionRatio -lt $AzureResults.CompressionRatio) { "AWS" } else { "Azure" }
        }
        
        $comparisonTable += @{
            Category   = "Size Metrics"
            Metric     = "Output File Count"
            AWS        = $AwsResults.OutputFileCount
            Azure      = $AzureResults.OutputFileCount
            Difference = [math]::Abs($AwsResults.OutputFileCount - $AzureResults.OutputFileCount)
            Winner     = "N/A"  # File count itself doesn't determine a winner
        }
    }
    
    # Add cost metrics
    $comparisonTable += @{
        Category   = "Cost Metrics (USD)"; 
        Metric     = ""; 
        AWS        = ""; 
        Azure      = ""; 
        Difference = ""; 
        Winner     = ""
    }
    
    if ($AwsResults -and $AzureResults) {
        $comparisonTable += @{
            Category   = "Cost Metrics (USD)"
            Metric     = "Storage Cost (1 day)"
            AWS        = [math]::Round($AwsResults.StorageCost, 5)
            Azure      = [math]::Round($AzureResults.StorageCost, 5)
            Difference = [math]::Round([math]::Abs($AwsResults.StorageCost - $AzureResults.StorageCost), 5)
            Winner     = if ($AwsResults.StorageCost -lt $AzureResults.StorageCost) { "AWS" } else { "Azure" }
        }
        
        $comparisonTable += @{
            Category   = "Cost Metrics (USD)"
            Metric     = "Request Cost"
            AWS        = [math]::Round($AwsResults.RequestCost, 5)
            Azure      = [math]::Round($AzureResults.RequestCost, 5)
            Difference = [math]::Round([math]::Abs($AwsResults.RequestCost - $AzureResults.RequestCost), 5)
            Winner     = if ($AwsResults.RequestCost -lt $AzureResults.RequestCost) { "AWS" } else { "Azure" }
        }
        
        $comparisonTable += @{
            Category   = "Cost Metrics (USD)"
            Metric     = "Total Estimated Cost"
            AWS        = [math]::Round($AwsResults.EstimatedCost, 5)
            Azure      = [math]::Round($AzureResults.EstimatedCost, 5)
            Difference = [math]::Round([math]::Abs($AwsResults.EstimatedCost - $AzureResults.EstimatedCost), 5)
            Winner     = if ($AwsResults.EstimatedCost -lt $AzureResults.EstimatedCost) { "AWS" } else { "Azure" }
        }
    }
    
    # Display the comparison table
    $currentCategory = ""
    foreach ($row in $comparisonTable) {
        if ($row.Metric -eq "") {
            # This is a category header
            $currentCategory = $row.Category
            Write-Host "`n$currentCategory" -ForegroundColor Yellow
            Write-Host "-" * $currentCategory.Length -ForegroundColor Yellow
            continue
        }
        
        # Format the winner with color
        $winnerDisplay = switch ($row.Winner) {
            "AWS" { "AWS" }
            "Azure" { "Azure" }
            default { "N/A" }
        }
        
        # Format the row
        $metricDisplay = $row.Metric.PadRight(25)
        $awsDisplay = $row.AWS.ToString().PadRight(15)
        $azureDisplay = $row.Azure.ToString().PadRight(15)
        $diffDisplay = $row.Difference.ToString().PadRight(15)
        
        # Display the row
        Write-Host "$metricDisplay" -NoNewline
        Write-Host "$awsDisplay" -NoNewline
        Write-Host "$azureDisplay" -NoNewline
        Write-Host "$diffDisplay" -NoNewline
        
        if ($row.Winner -eq "AWS") {
            Write-Host $winnerDisplay -ForegroundColor Green
        }
        elseif ($row.Winner -eq "Azure") {
            Write-Host $winnerDisplay -ForegroundColor Magenta
        }
        else {
            Write-Host $winnerDisplay
        }
    }
    
    # Calculate overall winner
    if ($AwsResults -and $AzureResults) {
        $awsWins = ($comparisonTable | Where-Object { $_.Winner -eq "AWS" }).Count
        $azureWins = ($comparisonTable | Where-Object { $_.Winner -eq "Azure" }).Count
        
        Write-Host "`nOverall Score: AWS: $awsWins, Azure: $azureWins" -ForegroundColor Yellow
        if ($awsWins -gt $azureWins) {
            Write-Host "Overall Winner: AWS" -ForegroundColor Green
        }
        elseif ($azureWins -gt $awsWins) {
            Write-Host "Overall Winner: Azure" -ForegroundColor Magenta
        }
        else {
            Write-Host "Overall Result: Tie" -ForegroundColor Yellow
        }
    }
    
    # Save results to JSON file
    $results = @{
        Date            = Get-Date
        AWS             = $AwsResults
        Azure           = $AzureResults
        ComparisonTable = $comparisonTable
    }
    
    $results | ConvertTo-Json -Depth 10 | Out-File $resultsFile
    Write-Host "`nDetailed results saved to: $resultsFile"
}

#################################################################
# MAIN SCRIPT
#################################################################

# Check prerequisites
Check-PrerequisiteCommands

# Initialize results
$awsResults = $null
$azureResults = $null

if ($testingOrder -eq "aws-first" -or $testingOrder -eq "both") {
    $awsResults = Test-AWS
}

if ($testingOrder -eq "azure-first" -or $testingOrder -eq "both") {
    $azureResults = Test-Azure
}

# Generate and display the comparison report
Show-ComparisonReport -AwsResults $awsResults -AzureResults $azureResults
