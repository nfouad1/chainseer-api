[CmdletBinding()]
param(
    # Robinhood produces roughly 3,000 blocks per five-minute interval.
    # 5,000 keeps discovery ahead while retaining a bounded RPC request.
    [ValidateRange(1, 10000)][int]$DiscoveryBlockLimit = 5000,
    [ValidateRange(0, 10)][int]$AnalysisLimit = 1,
    [ValidateRange(0, 100)][int]$OutcomeLimit = 12
)
$ErrorActionPreference = "Stop"
$workspacePath = $PSScriptRoot
$pythonPath = Join-Path $workspacePath ".venv\Scripts\python.exe"
$learningRoot = Join-Path $workspacePath "robinhood_learning"
$logRoot = Join-Path $learningRoot "logs"
$statusPath = Join-Path $learningRoot "scheduler_status.json"
$started = Get-Date
$mutex = [System.Threading.Mutex]::new($false, "Local\ChainseerRobinhoodLearnOnce")
$owned = $false
New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
$logPath = Join-Path $logRoot ("learn-once-{0}.log" -f (Get-Date -Format "yyyy-MM-dd"))
$stdoutPath = Join-Path $logRoot ("learn-once-{0}.stdout.tmp" -f $PID)
$stderrPath = Join-Path $logRoot ("learn-once-{0}.stderr.tmp" -f $PID)
function Write-Status([string]$status, [int]$exitCode, [string]$errorText) {
    $value = @{
        status=$status; started_at=$started.ToUniversalTime().ToString("o")
        completed_at=(Get-Date).ToUniversalTime().ToString("o")
        duration_seconds=[Math]::Round(((Get-Date)-$started).TotalSeconds,3)
        exit_code=$exitCode; last_error=$errorText; log_path=$logPath
        paper_only=$true; live_execution_enabled=$false
    }
    $temporary = "$statusPath.tmp"
    $value | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $temporary -Encoding utf8
    Move-Item -LiteralPath $temporary -Destination $statusPath -Force
}
try {
    $owned = $mutex.WaitOne(0)
    if (-not $owned) { Write-Status "skipped_overlap" 0 $null; exit 0 }
    Write-Status "running" 0 $null
    $arguments = @(
        "-X", "utf8", (Join-Path $workspacePath "chainseer_robinhood.py"),
        "learn-once", "--root", $learningRoot,
        "--discovery-block-limit", "$DiscoveryBlockLimit",
        "--analysis-limit", "$AnalysisLimit", "--outcome-limit", "$OutcomeLimit"
    )
    $process = Start-Process -FilePath $pythonPath -ArgumentList $arguments -Wait `
        -PassThru -NoNewWindow -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath
    $stdout = if (Test-Path -LiteralPath $stdoutPath) { Get-Content -LiteralPath $stdoutPath -Raw } else { "" }
    $stderr = if (Test-Path -LiteralPath $stderrPath) { Get-Content -LiteralPath $stderrPath -Raw } else { "" }
    if ($stdout) { Add-Content -LiteralPath $logPath -Value $stdout }
    if ($stderr) { Add-Content -LiteralPath $logPath -Value $stderr }
    if ($process.ExitCode -ne 0) {
        $detail = if ($stderr) { $stderr.Trim() } else { "Robinhood learner exited with code $($process.ExitCode)" }
        Write-Status "failed" $process.ExitCode $detail
        throw $detail
    }
    Write-Status "complete" 0 $null
}
catch {
    if (-not (Test-Path -LiteralPath $statusPath) -or
        (Get-Content -LiteralPath $statusPath -Raw | ConvertFrom-Json).status -ne "failed") {
        Write-Status "failed" 1 $_.Exception.ToString()
    }
    throw
}
finally {
    Remove-Item -LiteralPath $stdoutPath -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $stderrPath -Force -ErrorAction SilentlyContinue
    if ($owned) { $mutex.ReleaseMutex() }
    $mutex.Dispose()
}
