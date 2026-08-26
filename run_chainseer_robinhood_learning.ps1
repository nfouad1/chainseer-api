[CmdletBinding()]
param(
    # Historical work is isolated from the live head. A committed 1,000-block
    # chunk now exceeds measured chain growth while remaining far below the
    # 5,000-block attempts that repeatedly reached the
    # lane deadline before it can advance its durable cursor.
    [ValidateRange(1, 10000)][int]$DiscoveryBlockLimit = 1000,
    # Analysis is the binding stage. At 2 per five-minute cycle it processed
    # 24 candidates/hour against 55/hour of discovery, so the pending queue
    # grew ~31/hour and stood at 188; entry starved to 0.6 admits/hour while
    # the book drained 27 -> 17 by attrition. Cycles used 34s of a 300s budget
    # at the old limit, so the constraint was the cap, not the clock.
    # Raised to 8 (96/hour) to clear the backlog and outpace discovery.
    [ValidateRange(0, 10)][int]$AnalysisLimit = 8,
    # Timely checkpoints retain learning value and always receive this slice.
    # At the measured discovery rate, 12 per cycle covers fresh demand without
    # letting remote market lookups consume the complete five-minute cycle.
    [ValidateRange(0, 100)][int]$OutcomeLimit = 12,
    # Late checkpoints are coverage-only observations. Keep their recovery
    # separate and small so an outage backlog cannot starve timely marks.
    [ValidateRange(0, 30)][int]$OutcomeRecoveryLimit = 4,
    # Market-only checks are cheap; full analysis runs only after a verified
    # executable pool is found.
    [ValidateRange(0, 20)][int]$MarketRecheckLimit = 4,
    # The task repeats every five minutes. Leave a small clean handoff window
    # so a late task invocation never overlaps the next Job Object owner.
    [ValidateRange(60, 295)][int]$SupervisorWindowSeconds = 285
)
$ErrorActionPreference = "Stop"
$workspacePath = $PSScriptRoot
$bootstrapPath = Join-Path $workspacePath "robinhood_learning\runner_bootstrap.log"
New-Item -ItemType Directory -Path (Split-Path -Parent $bootstrapPath) -Force | Out-Null
function Write-Bootstrap([string]$phase) {
    Add-Content -LiteralPath $bootstrapPath -Encoding utf8 -Value (
        "{0}`tpid={1}`t{2}" -f (Get-Date).ToUniversalTime().ToString("o"), $PID, $phase
    )
}
Write-Bootstrap "script_entry"
# The venv executable is a uv launcher which spawns the real interpreter. A
# Job Object can own the launcher while that second process breaks away, so a
# killed task can still orphan the learner. Do not even invoke that launcher
# during scheduled startup: under Task Scheduler it can block before status or
# ownership is established. Resolve the base interpreter from pyvenv.cfg and
# launch it directly with the venv packages: one OS process, one owner.
$venvConfigPath = Join-Path $workspacePath ".venv\pyvenv.cfg"
$venvHomeLine = Get-Content -LiteralPath $venvConfigPath | Where-Object {
    $_ -match '^\s*home\s*='
} | Select-Object -First 1
if (-not $venvHomeLine) { throw "Python home is missing from $venvConfigPath" }
$pythonHome = ($venvHomeLine -split '=', 2)[1].Trim()
$pythonPath = Join-Path $pythonHome "python.exe"
if (-not (Test-Path -LiteralPath $pythonPath -PathType Leaf)) {
    throw "Base Python interpreter not found: $pythonPath"
}
Write-Bootstrap "base_python_resolved"
$venvSitePackages = Join-Path $workspacePath ".venv\Lib\site-packages"
$previousPythonPath = $env:PYTHONPATH
$env:PYTHONPATH = if ($previousPythonPath) {
    "$venvSitePackages;$previousPythonPath"
} else {
    $venvSitePackages
}
$learningRoot = Join-Path $workspacePath "robinhood_learning"
$logRoot = Join-Path $learningRoot "logs"
$statusPath = Join-Path $learningRoot "runner_status.json"
$started = Get-Date
$mutex = [System.Threading.Mutex]::new($false, "Local\ChainseerRobinhoodLearnOnce")
$owned = $false
New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
$logPath = Join-Path $logRoot ("learn-once-{0}.log" -f (Get-Date -Format "yyyy-MM-dd"))
$stdoutPath = Join-Path $logRoot ("learn-once-{0}.stdout.tmp" -f $PID)
$stderrPath = Join-Path $logRoot ("learn-once-{0}.stderr.tmp" -f $PID)
function Write-Status([string]$status, [int]$exitCode, [string]$errorText) {
    # A running cycle has not completed and has no exit code. Stamping both
    # made "running" indistinguishable from "finished successfully" to any
    # reader, which is how a 1,714-second cycle could look complete.
    $terminal = $status -ne "running"
    $value = @{
        status=$status; started_at=$started.ToUniversalTime().ToString("o")
        completed_at=$(if ($terminal) { (Get-Date).ToUniversalTime().ToString("o") } else { $null })
        duration_seconds=[Math]::Round(((Get-Date)-$started).TotalSeconds,3)
        exit_code=$(if ($terminal) { $exitCode } else { $null })
        last_error=$errorText; log_path=$logPath
        paper_only=$true; live_execution_enabled=$false
    }
    $temporary = "$statusPath.tmp"
    $value | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $temporary -Encoding utf8
    Move-Item -LiteralPath $temporary -Destination $statusPath -Force
}
try {
    $owned = $mutex.WaitOne(0)
    if (-not $owned) {
        # A skip must not touch the ACTIVE run's status. Writing
        # "skipped_overlap" to the shared status file overwrote the running
        # cycle's own state, which is why external status was unreliable:
        # with cycles at 150s and the task firing every 5 minutes, most
        # invocations skip, and each one clobbered the truth.
        $skipPath = Join-Path (Split-Path -Parent $statusPath) "last_skip.json"
        @{ status = "skipped_overlap"; at = (Get-Date).ToUniversalTime().ToString("o") } |
            ConvertTo-Json | Set-Content -LiteralPath $skipPath -Encoding utf8
        exit 0
    }
    Write-Bootstrap "mutex_acquired"
    Write-Status "running" 0 $null
    Write-Bootstrap "status_running"
    $arguments = @(
        "-X", "utf8", (Join-Path $workspacePath "chainseer_robinhood.py"),
        "lanes", "--root", $learningRoot,
        "--duration-seconds", "$SupervisorWindowSeconds",
        "--discovery-block-limit", "$DiscoveryBlockLimit",
        "--analysis-limit", "$AnalysisLimit", "--outcome-limit", "$OutcomeLimit",
        "--outcome-recovery-limit", "$OutcomeRecoveryLimit",
        "--market-recheck-limit", "$MarketRecheckLimit"
    )
    # Own the whole tree. Start-Process -Wait waits, but if this PowerShell is
    # terminated the Python child is orphaned and keeps holding the learning
    # lock and the database. A Job Object with
    # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE makes the OS destroy the child when
    # the parent's handle closes, however the parent dies.
    if (-not ("ChainseerJob" -as [type])) {
        Add-Type -Name ChainseerJob -Namespace Win32 -MemberDefinition @"
[DllImport("kernel32.dll", CharSet=CharSet.Unicode)]
public static extern IntPtr CreateJobObject(IntPtr a, string lpName);
[DllImport("kernel32.dll")]
public static extern bool SetInformationJobObject(IntPtr hJob, int infoClass, IntPtr lpInfo, uint cbInfo);
[DllImport("kernel32.dll")]
public static extern bool AssignProcessToJobObject(IntPtr hJob, IntPtr hProc);
"@
    }
    $job = [Win32.ChainseerJob]::CreateJobObject([IntPtr]::Zero, $null)
    # JOBOBJECT_EXTENDED_LIMIT_INFORMATION: LimitFlags at offset 16 on x64,
    # 0x2000 = KILL_ON_JOB_CLOSE.
    $infoSize = 144
    $info = [Runtime.InteropServices.Marshal]::AllocHGlobal($infoSize)
    # AllocHGlobal returns uninitialized native memory. Leaving the remaining
    # fields as random bytes can accidentally enable CPU/process limits and,
    # under Task Scheduler, was observed to leave the Python child suspended
    # before it wrote its first heartbeat. Zero the complete structure and set
    # only the one limit we intentionally own.
    for ($offset = 0; $offset -lt $infoSize; $offset++) {
        [Runtime.InteropServices.Marshal]::WriteByte($info, $offset, 0)
    }
    [Runtime.InteropServices.Marshal]::WriteInt32($info, 16, 0x2000)
    [void][Win32.ChainseerJob]::SetInformationJobObject($job, 9, $info, $infoSize)

    # NOT -Wait. Start-Process -Wait returns only after the child has already
    # exited, so assigning it to the job afterwards was dead code and the
    # process tree was never owned. Start detached-but-tracked, assign to the
    # job immediately, THEN wait.
    $process = Start-Process -FilePath $pythonPath -ArgumentList $arguments `
        -PassThru -NoNewWindow -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath
    if (-not $process) { throw "learner process failed to start" }
    Write-Bootstrap ("child_started child_pid={0}" -f $process.Id)
    $assigned = [Win32.ChainseerJob]::AssignProcessToJobObject($job, $process.Handle)
    if (-not $assigned) {
        # Refusing to proceed unowned is deliberate: an unowned child is the
        # orphan this whole change exists to prevent.
        try { $process.Kill() } catch { }
        throw "could not assign learner to job object; refusing to run unowned"
    }
    Write-Bootstrap ("child_job_assigned child_pid={0}" -f $process.Id)
    $process.WaitForExit()
    Write-Bootstrap ("child_exited child_pid={0} exit_code={1}" -f $process.Id, $process.ExitCode)
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
    $env:PYTHONPATH = $previousPythonPath
    Remove-Item -LiteralPath $stdoutPath -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $stderrPath -Force -ErrorAction SilentlyContinue
    if ($owned) { $mutex.ReleaseMutex() }
    $mutex.Dispose()
}
