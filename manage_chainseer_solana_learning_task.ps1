[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet(
        "install",
        "reschedule",
        "start",
        "stop",
        "status",
        "run-now",
        "uninstall"
    )]
    [string]$Command = "status",

    [ValidateRange(1, 1440)]
    [int]$IntervalMinutes = 10
)

$ErrorActionPreference = "Stop"
$taskName = "Chainseer Solana Learn Once"
$runnerPath = Join-Path $PSScriptRoot "run_chainseer_solana_learning.ps1"
$learningRoot = Join-Path $PSScriptRoot "solana_learning"
$schedulePath = Join-Path $learningRoot "schedule.json"
$statusPath = Join-Path $learningRoot "scheduler_status.json"
$powershellPath = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"

function Get-ChainseerSolanaTask {
    Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
}

function Get-ChainseerSolanaIntervalMinutes {
    $task = Get-ChainseerSolanaTask
    if ($null -eq $task) {
        return $IntervalMinutes
    }
    $interval = [string]$task.Triggers[0].Repetition.Interval
    if ([string]::IsNullOrWhiteSpace($interval)) {
        return $IntervalMinutes
    }
    try {
        return [int][Math]::Round(
            [System.Xml.XmlConvert]::ToTimeSpan($interval).TotalMinutes
        )
    }
    catch {
        return $IntervalMinutes
    }
}

function Write-ScheduleState {
    param(
        [bool]$Installed,
        [bool]$Enabled
    )
    $effectiveRpcUrl = $env:CHAINSEER_SOLANA_RPC_URL
    if ([string]::IsNullOrWhiteSpace($effectiveRpcUrl)) {
        $effectiveRpcUrl = [Environment]::GetEnvironmentVariable(
            "CHAINSEER_SOLANA_RPC_URL",
            "User"
        )
    }
    New-Item -ItemType Directory -Path $learningRoot -Force | Out-Null
    $temporaryPath = "$schedulePath.tmp"
    @{
        task_name = $taskName
        installed = $Installed
        enabled = $Enabled
        interval_minutes = Get-ChainseerSolanaIntervalMinutes
        updated_at = (Get-Date).ToUniversalTime().ToString("o")
        runner_path = $runnerPath
        jupiter_access_mode = if (
            [string]::IsNullOrWhiteSpace($env:JUPITER_API_KEY)
        ) { "keyless" } else { "api_key" }
        rpc_mode = if (
            [string]::IsNullOrWhiteSpace($effectiveRpcUrl)
        ) { "public_default" } else { "configured" }
        paper_only = $true
        live_execution_enabled = $false
    } | ConvertTo-Json -Depth 8 |
        Set-Content -LiteralPath $temporaryPath -Encoding utf8
    Move-Item -LiteralPath $temporaryPath -Destination $schedulePath -Force
}

function Show-ChainseerSolanaTaskStatus {
    $task = Get-ChainseerSolanaTask
    if ($null -eq $task) {
        Write-Output "Task '$taskName' is not installed."
        return
    }
    $info = Get-ScheduledTaskInfo -TaskName $taskName
    [pscustomobject]@{
        TaskName = $task.TaskName
        State = $task.State
        LastRunTime = $info.LastRunTime
        LastTaskResult = $info.LastTaskResult
        NextRunTime = $info.NextRunTime
        UserId = $task.Principal.UserId
        LogonType = $task.Principal.LogonType
        RunLevel = $task.Principal.RunLevel
        MultipleInstances = $task.Settings.MultipleInstances
        ExecutionTimeLimit = $task.Settings.ExecutionTimeLimit
        IntervalMinutes = Get-ChainseerSolanaIntervalMinutes
        Action = $task.Actions[0].Execute
        Arguments = $task.Actions[0].Arguments
    } | Format-List
    if (Test-Path -LiteralPath $statusPath -PathType Leaf) {
        Write-Output "Latest learner status:"
        Get-Content -Raw -LiteralPath $statusPath
    }
}

function Install-ChainseerSolanaTask {
    if (-not (Test-Path -LiteralPath $runnerPath -PathType Leaf)) {
        throw "Solana learning runner was not found: $runnerPath"
    }
    $existing = Get-ChainseerSolanaTask
    if ($null -ne $existing) {
        $existingArguments = [string]$existing.Actions[0].Arguments
        if (-not $existingArguments.Contains($runnerPath)) {
            throw (
                "Task '$taskName' exists but points elsewhere; " +
                "refusing to replace it."
            )
        }
    }

    $userId = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
    $arguments = (
        '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "{0}"'
    ) -f $runnerPath
    $action = New-ScheduledTaskAction `
        -Execute $powershellPath `
        -Argument $arguments `
        -WorkingDirectory $PSScriptRoot
    $trigger = New-ScheduledTaskTrigger `
        -Once `
        -At (Get-Date).AddMinutes(1) `
        -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes) `
        -RepetitionDuration (New-TimeSpan -Days 3650)
    $settings = New-ScheduledTaskSettingsSet `
        -MultipleInstances IgnoreNew `
        -StartWhenAvailable `
        -ExecutionTimeLimit (New-TimeSpan -Minutes 30) `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries
    $principal = New-ScheduledTaskPrincipal `
        -UserId $userId `
        -LogonType Interactive `
        -RunLevel Limited
    $definition = New-ScheduledTask `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Principal $principal `
        -Description (
            "Paper-only Chainseer Solana Pump.fun discovery, tri-state risk " +
            "analysis, shadow marking, promotion evidence, and Timechain sealing."
        )
    Register-ScheduledTask `
        -TaskName $taskName `
        -InputObject $definition `
        -Force | Out-Null
    Write-ScheduleState -Installed $true -Enabled $true
}

switch ($Command) {
    "install" {
        Install-ChainseerSolanaTask
        Start-ScheduledTask -TaskName $taskName
        Write-Output (
            "Installed and started '$taskName'. It repeats every " +
            "$IntervalMinutes minutes while the current user is logged on."
        )
        Show-ChainseerSolanaTaskStatus
    }
    "start" {
        if ($null -eq (Get-ChainseerSolanaTask)) {
            Install-ChainseerSolanaTask
        }
        Enable-ScheduledTask -TaskName $taskName | Out-Null
        Write-ScheduleState -Installed $true -Enabled $true
        Start-ScheduledTask -TaskName $taskName
        Write-Output "Enabled and started '$taskName'."
        Show-ChainseerSolanaTaskStatus
    }
    "reschedule" {
        Install-ChainseerSolanaTask
        Enable-ScheduledTask -TaskName $taskName | Out-Null
        Write-ScheduleState -Installed $true -Enabled $true
        Write-Output (
            "Rescheduled '$taskName' to every $IntervalMinutes minute(s). " +
            "The next natural trigger will start it."
        )
        Show-ChainseerSolanaTaskStatus
    }
    "stop" {
        if ($null -ne (Get-ChainseerSolanaTask)) {
            Stop-ScheduledTask `
                -TaskName $taskName `
                -ErrorAction SilentlyContinue
            Disable-ScheduledTask -TaskName $taskName | Out-Null
        }
        Write-ScheduleState `
            -Installed ($null -ne (Get-ChainseerSolanaTask)) `
            -Enabled $false
        Write-Output "Stopped and disabled '$taskName'."
        Show-ChainseerSolanaTaskStatus
    }
    "status" {
        Show-ChainseerSolanaTaskStatus
    }
    "run-now" {
        if ($null -eq (Get-ChainseerSolanaTask)) {
            throw "Task '$taskName' is not installed."
        }
        Start-ScheduledTask -TaskName $taskName
        Write-Output "Started one '$taskName' cycle."
        Show-ChainseerSolanaTaskStatus
    }
    "uninstall" {
        if ($null -eq (Get-ChainseerSolanaTask)) {
            Write-Output "Task '$taskName' is already absent."
            Write-ScheduleState -Installed $false -Enabled $false
            break
        }
        Stop-ScheduledTask `
            -TaskName $taskName `
            -ErrorAction SilentlyContinue
        Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
        Write-ScheduleState -Installed $false -Enabled $false
        Write-Output (
            "Removed '$taskName'. Solana runtime data, ledgers, " +
            "Timechain, and logs were preserved."
        )
    }
}
