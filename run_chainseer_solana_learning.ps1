[CmdletBinding()]
param(
    [ValidateRange(1, 25)]
    [int]$Limit = 3,

    [ValidateRange(1, 1000)]
    [int]$SignatureLimit = 100,

    [ValidateRange(0, 25)]
    [int]$RecoveryLimit = 3,

    [ValidateRange(0, 25)]
    [int]$GraduationLimit = 6
)

$ErrorActionPreference = "Stop"
$workspacePath = $PSScriptRoot
$pythonPath = "C:\Users\sonas\AppData\Local\Python\pythoncore-3.14-64\python.exe"
$chainseerPath = Join-Path $workspacePath "chainseer_solana.py"
$learningRoot = Join-Path $workspacePath "solana_learning"
$chainRoot = Join-Path $workspacePath "solana_chain"
$logRoot = Join-Path $learningRoot "logs"
$statusPath = Join-Path $learningRoot "scheduler_status.json"
$summaryPath = Join-Path $learningRoot "learning_summary.json"
$controllerPath = Join-Path $learningRoot "controller_status.json"
$reflectionPath = Join-Path $learningRoot "reflection_state.json"
$locationPushed = $false
$mutex = $null
$mutexOwned = $false
$exitCode = $null
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
    $Value | ConvertTo-Json -Depth 10 |
        Set-Content -LiteralPath $temporaryPath -Encoding utf8
    for ($attempt = 0; $attempt -lt 6; $attempt++) {
        try {
            Move-Item -LiteralPath $temporaryPath -Destination $Path -Force
            return
        }
        catch {
            if ($attempt -eq 5) { throw }
            Start-Sleep -Milliseconds (50 * [Math]::Pow(2, $attempt))
        }
    }
}

function Read-JsonFile {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { return $null }
    try {
        return Get-Content -Raw -LiteralPath $Path | ConvertFrom-Json
    }
    catch {
        return $null
    }
}

function Write-ControllerState {
    param(
        [string]$Status,
        [datetime]$At,
        [int]$CompletedDelta = 0,
        [int]$FailedDelta = 0,
        [int]$SkippedDelta = 0,
        [int]$AnalyzedDelta = 0,
        [int]$LaunchDelta = 0,
        [string]$Reason = $null
    )
    $old = Read-JsonFile -Path $controllerPath
    Write-AtomicJson -Path $controllerPath -Value @{
        schema_version = 1
        status = $Status
        cycles_started = [int]$(if ($null -ne $old) { $old.cycles_started } else { 0 }) + $(if ($Status -eq "running") { 1 } else { 0 })
        cycles_completed = [int]$(if ($null -ne $old) { $old.cycles_completed } else { 0 }) + $CompletedDelta
        cycles_failed = [int]$(if ($null -ne $old) { $old.cycles_failed } else { 0 }) + $FailedDelta
        cycles_skipped = [int]$(if ($null -ne $old) { $old.cycles_skipped } else { 0 }) + $SkippedDelta
        analyses_committed = [int]$(if ($null -ne $old) { $old.analyses_committed } else { 0 }) + $AnalyzedDelta
        launches_discovered = [int]$(if ($null -ne $old) { $old.launches_discovered } else { 0 }) + $LaunchDelta
        last_started_at = $(if ($Status -eq "running") { $At.ToUniversalTime().ToString("o") } elseif ($null -ne $old) { $old.last_started_at } else { $null })
        last_completed_at = $(if ($CompletedDelta -gt 0) { $At.ToUniversalTime().ToString("o") } elseif ($null -ne $old) { $old.last_completed_at } else { $null })
        last_failed_at = $(if ($FailedDelta -gt 0) { $At.ToUniversalTime().ToString("o") } elseif ($null -ne $old) { $old.last_failed_at } else { $null })
        last_skip_at = $(if ($SkippedDelta -gt 0) { $At.ToUniversalTime().ToString("o") } elseif ($null -ne $old) { $old.last_skip_at } else { $null })
        last_reason = $Reason
        analysis_interval = 200
        paper_only = $true
        live_execution_enabled = $false
        updated_at = $At.ToUniversalTime().ToString("o")
    }
}

if (-not (Test-Path -LiteralPath $pythonPath -PathType Leaf)) {
    throw "Chainseer Python runtime was not found: $pythonPath"
}
if (-not (Test-Path -LiteralPath $chainseerPath -PathType Leaf)) {
    throw "Solana adapter entry point was not found: $chainseerPath"
}

New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
$userRpcUrl = [Environment]::GetEnvironmentVariable(
    "CHAINSEER_SOLANA_RPC_URL",
    "User"
)
if (
    [string]::IsNullOrWhiteSpace($env:CHAINSEER_SOLANA_RPC_URL) -and
    -not [string]::IsNullOrWhiteSpace($userRpcUrl)
) {
    $env:CHAINSEER_SOLANA_RPC_URL = $userRpcUrl
}
$userRpcFallbacks = [Environment]::GetEnvironmentVariable(
    "CHAINSEER_SOLANA_RPC_FALLBACK_URLS",
    "User"
)
if (
    [string]::IsNullOrWhiteSpace(
        $env:CHAINSEER_SOLANA_RPC_FALLBACK_URLS
    ) -and
    -not [string]::IsNullOrWhiteSpace($userRpcFallbacks)
) {
    $env:CHAINSEER_SOLANA_RPC_FALLBACK_URLS = $userRpcFallbacks
}
$logPath = Join-Path $logRoot (
    "learn-once-{0}.log" -f (Get-Date -Format "yyyy-MM-dd")
)
$accessMode = if ([string]::IsNullOrWhiteSpace($env:JUPITER_API_KEY)) {
    "keyless"
}
else {
    "api_key"
}
$rpcMode = if (
    [string]::IsNullOrWhiteSpace($env:CHAINSEER_SOLANA_RPC_URL)
) {
    "public_default"
}
else {
    "configured"
}

try {
    $mutex = [System.Threading.Mutex]::new(
        $false,
        "Local\ChainseerSolanaLearnOnce"
    )
    $mutexOwned = $mutex.WaitOne(0)
    if (-not $mutexOwned) {
        "[$startedAt] SKIP overlapping Solana learn-once is already running" |
            Out-File -LiteralPath $logPath -Append -Encoding utf8
        exit 0
    }
    $reflection = Read-JsonFile -Path $reflectionPath
    if (
        $null -ne $reflection -and
        ($reflection.pause_requested -eq $true -or $reflection.status -eq "pending")
    ) {
        Write-ControllerState `
            -Status "reflection_pending" `
            -At (Get-Date) `
            -SkippedDelta 1 `
            -Reason "sealed_reflection_checkpoint_pending"
        Write-AtomicJson -Path $statusPath -Value @{
            status = "reflection_pending"
            started_at = $startedAt
            completed_at = $startedAt
            exit_code = 0
            duration_seconds = 0
            log_path = $logPath
            last_error = $null
            reflection_checkpoint_id = $reflection.pending_checkpoint.checkpoint_id
            paper_only = $true
            live_execution_enabled = $false
        }
        "[$startedAt] SKIP sealed recursive-learning reflection checkpoint pending" |
            Out-File -LiteralPath $logPath -Append -Encoding utf8
        exit 0
    }
    Write-ControllerState -Status "running" -At $started

    Write-AtomicJson -Path $statusPath -Value @{
        status = "running"
        started_at = $startedAt
        completed_at = $null
        exit_code = $null
        duration_seconds = $null
        log_path = $logPath
        last_error = $null
        jupiter_access_mode = $accessMode
        rpc_mode = $rpcMode
        limit = $Limit
        signature_limit = $SignatureLimit
        recovery_limit = $RecoveryLimit
        paper_only = $true
        live_execution_enabled = $false
    }

    Push-Location -LiteralPath $workspacePath
    $locationPushed = $true
    $env:PYTHONUTF8 = "1"

    "[$startedAt] START solana learn-once access=$accessMode rpc=$rpcMode limit=$Limit signatures=$SignatureLimit" |
        Out-File -LiteralPath $logPath -Append -Encoding utf8
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        # Preserve complete native stderr in the daily diagnostic log.
        $ErrorActionPreference = "Continue"
        $output = @(
            & $pythonPath -X utf8 $chainseerPath `
                --root $learningRoot `
                --chain-root $chainRoot `
                learn-once `
                --limit $Limit `
                --signature-limit $SignatureLimit `
                --recovery-limit $RecoveryLimit `
                --graduation-limit $GraduationLimit 2>&1
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
    "[$finishedAt] END solana learn-once exit_code=$exitCode duration=${duration}s" |
        Out-File -LiteralPath $logPath -Append -Encoding utf8

    if ($exitCode -ne 0) {
        $diagnostic = (
            $output |
                Select-Object -Last 12 |
                ForEach-Object { $_.ToString() }
        ) -join [Environment]::NewLine
        throw (
            "Solana learn-once exited with code $exitCode. " +
            "$diagnostic See $logPath"
        )
    }

    $cycle = $null
    if (Test-Path -LiteralPath $summaryPath -PathType Leaf) {
        try {
            $cycle = Get-Content -Raw -LiteralPath $summaryPath |
                ConvertFrom-Json
        }
        catch {
            $cycle = $null
        }
    }
    Write-AtomicJson -Path $statusPath -Value @{
        status = "complete"
        started_at = $startedAt
        completed_at = $finishedAt
        exit_code = $exitCode
        duration_seconds = $duration
        log_path = $logPath
        last_error = $null
        jupiter_access_mode = $accessMode
        rpc_mode = $rpcMode
        limit = $Limit
        signature_limit = $SignatureLimit
        recovery_limit = $RecoveryLimit
        analyzed = $(if ($null -ne $cycle) { $cycle.cycle.analyzed } else { $null })
        new_launches = $(if ($null -ne $cycle) { $cycle.cycle.new_launches } else { $null })
        shadow_open = $(if ($null -ne $cycle) { $cycle.cycle.shadow_open } else { $null })
        shadow_closed = $(if ($null -ne $cycle) { $cycle.cycle.shadow_closed } else { $null })
        paper_only = $true
        live_execution_enabled = $false
    }
    Write-ControllerState `
        -Status "complete" `
        -At $finished `
        -CompletedDelta 1 `
        -AnalyzedDelta $(if ($null -ne $cycle) { [int]$cycle.cycle.analyzed } else { 0 }) `
        -LaunchDelta $(if ($null -ne $cycle) { [int]$cycle.cycle.new_launches } else { 0 }) `
        -Reason "cycle_completed"
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
        jupiter_access_mode = $accessMode
        rpc_mode = $rpcMode
        limit = $Limit
        signature_limit = $SignatureLimit
        recovery_limit = $RecoveryLimit
        paper_only = $true
        live_execution_enabled = $false
    }
    Write-ControllerState `
        -Status "failed" `
        -At $failed `
        -FailedDelta 1 `
        -Reason $message
    Write-Error $_
    exit 1
}
finally {
    if ($locationPushed) {
        Pop-Location
    }
    if ($mutexOwned -and $null -ne $mutex) {
        $mutex.ReleaseMutex()
    }
    if ($null -ne $mutex) {
        $mutex.Dispose()
    }
}

exit 0
