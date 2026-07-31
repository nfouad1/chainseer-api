[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$workspacePath = $PSScriptRoot
$pythonPath = "C:\Users\sonas\AppData\Local\Python\pythoncore-3.14-64\python.exe"
$chainseerPath = Join-Path $workspacePath "chainseer_pons.py"
$learningRoot = Join-Path $workspacePath "pons_learning"
$chainRoot = Join-Path $workspacePath "pons_chain"
$logRoot = Join-Path $learningRoot "logs"
$statusPath = Join-Path $learningRoot "guard_status.json"
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
$logPath = Join-Path $logRoot (
    "guard-once-{0}.log" -f (Get-Date -Format "yyyy-MM-dd")
)

Write-AtomicJson -Path $statusPath -Value @{
    status = "running"
    started_at = $startedAt
    completed_at = $null
    exit_code = $null
    duration_seconds = $null
    log_path = $logPath
    last_error = $null
    paper_only = $true
    live_execution_enabled = $false
}

try {
    Push-Location -LiteralPath $workspacePath
    $locationPushed = $true
    "[$startedAt] START pons guard-once" |
        Out-File -LiteralPath $logPath -Append -Encoding utf8
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            & $pythonPath -X utf8 $chainseerPath guard-once `
                --guard-limit 25 `
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
    if ($exitCode -ne 0) {
        throw "Pons guard-once exited with code $exitCode."
    }
    $finished = Get-Date
    $finishedAt = $finished.ToUniversalTime().ToString("o")
    $duration = [Math]::Round(($finished - $started).TotalSeconds, 3)
    $outputText = (
        $output | ForEach-Object { $_.ToString() }
    ) -join [Environment]::NewLine
    $skipped = $outputText -match "pons-guard-once: skipped_busy"
    "[$finishedAt] END pons guard-once exit_code=0 duration=${duration}s" |
        Out-File -LiteralPath $logPath -Append -Encoding utf8
    Write-AtomicJson -Path $statusPath -Value @{
        status = $(if ($skipped) { "skipped_busy" } else { "complete" })
        started_at = $startedAt
        completed_at = $finishedAt
        exit_code = 0
        duration_seconds = $duration
        log_path = $logPath
        last_error = $null
        paper_only = $true
        live_execution_enabled = $false
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
        exit_code = 1
        duration_seconds = $duration
        log_path = $logPath
        last_error = $message
        paper_only = $true
        live_execution_enabled = $false
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
