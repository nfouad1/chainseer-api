[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$workspacePath = $PSScriptRoot
$pythonPath = "C:\Users\sonas\AppData\Local\Python\pythoncore-3.14-64\python.exe"
$chainseerPath = Join-Path $workspacePath "chainseer_pons.py"
$learningRoot = Join-Path $workspacePath "pons_learning"
$chainRoot = Join-Path $workspacePath "pons_chain"
$logRoot = Join-Path $learningRoot "logs"
$statusPath = Join-Path $learningRoot "scheduler_status.json"
$locationPushed = $false
$started = Get-Date
$startedAt = $started.ToUniversalTime().ToString("o")

function Write-AtomicJson {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [hashtable]$Value
    )
    $temporaryPath = "$Path.tmp"
    $Value | ConvertTo-Json -Depth 8 |
        Set-Content -LiteralPath $temporaryPath -Encoding utf8
    Move-Item -LiteralPath $temporaryPath -Destination $Path -Force
}

if (-not (Test-Path -LiteralPath $pythonPath -PathType Leaf)) {
    throw "Chainseer Python runtime was not found: $pythonPath"
}
if (-not (Test-Path -LiteralPath $chainseerPath -PathType Leaf)) {
    throw "Pons adapter entry point was not found: $chainseerPath"
}

New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
$logPath = Join-Path $logRoot ("learn-once-{0}.log" -f (Get-Date -Format "yyyy-MM-dd"))

Write-AtomicJson -Path $statusPath -Value @{
    status = "running"
    started_at = $startedAt
    completed_at = $null
    exit_code = $null
    duration_seconds = $null
    log_path = $logPath
    last_error = $null
}

try {
    Push-Location -LiteralPath $workspacePath
    $locationPushed = $true

    "[$startedAt] START pons learn-once" |
        Out-File -LiteralPath $logPath -Append -Encoding utf8
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        # Windows PowerShell surfaces native stderr as ErrorRecord objects.
        # Keep collecting so Python's complete traceback reaches the log.
        $ErrorActionPreference = "Continue"
        $output = @(
            & $pythonPath -X utf8 $chainseerPath learn-once `
                --limit 2 `
                --mark-limit 1 `
                --admission-refresh-limit 1 `
                --max-chunks 2 `
                --amount-eth 0.01 `
                --root $learningRoot `
                --chain-root $chainRoot 2>&1
        )
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    if ($output.Count -gt 0) {
        $output | ForEach-Object { $_.ToString() } |
            Out-File -LiteralPath $logPath -Append -Encoding utf8
    }

    $finished = Get-Date
    $finishedAt = $finished.ToUniversalTime().ToString("o")
    $duration = [Math]::Round(($finished - $started).TotalSeconds, 3)
    "[$finishedAt] END pons learn-once exit_code=$exitCode duration=${duration}s" |
        Out-File -LiteralPath $logPath -Append -Encoding utf8

    if ($exitCode -ne 0) {
        $diagnostic = (
            $output |
                Select-Object -Last 12 |
                ForEach-Object { $_.ToString() }
        ) -join [Environment]::NewLine
        throw (
            "Pons learn-once exited with code $exitCode. " +
            "$diagnostic See $logPath"
        )
    }

    Write-AtomicJson -Path $statusPath -Value @{
        status = "complete"
        started_at = $startedAt
        completed_at = $finishedAt
        exit_code = $exitCode
        duration_seconds = $duration
        log_path = $logPath
        last_error = $null
    }
}
catch {
    $failed = Get-Date
    $failedAt = $failed.ToUniversalTime().ToString("o")
    $duration = [Math]::Round(($failed - $started).TotalSeconds, 3)
    $message = $_.Exception.Message
    "[$failedAt] ERROR $message" |
        Out-File -LiteralPath $logPath -Append -Encoding utf8
    Write-AtomicJson -Path $statusPath -Value @{
        status = "failed"
        started_at = $startedAt
        completed_at = $failedAt
        exit_code = $(if ($null -ne $exitCode) { $exitCode } else { 1 })
        duration_seconds = $duration
        log_path = $logPath
        last_error = $message
    }
    Write-Error $_
    exit 1
}
finally {
    if ($locationPushed) {
        Pop-Location
    }
}

exit 0
