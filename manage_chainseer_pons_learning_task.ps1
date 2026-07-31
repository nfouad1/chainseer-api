[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet("install", "enable", "disable", "status", "run-now", "uninstall")]
    [string]$Command = "status",

    [ValidateRange(5, 1440)]
    [int]$IntervalMinutes = 10
)

$ErrorActionPreference = "Stop"
$taskName = "Chainseer Pons Learn Once"
$runnerPath = Join-Path $PSScriptRoot "run_chainseer_pons_learning.ps1"
$learningRoot = Join-Path $PSScriptRoot "pons_learning"
$schedulePath = Join-Path $learningRoot "schedule.json"
$powershellPath = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"

function Get-ChainseerPonsTask {
    Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
}

function Write-ScheduleState {
    param(
        [bool]$Installed,
        [bool]$Enabled
    )
    New-Item -ItemType Directory -Path $learningRoot -Force | Out-Null
    $temporaryPath = "$schedulePath.tmp"
    @{
        task_name = $taskName
        installed = $Installed
        enabled = $Enabled
        interval_minutes = $IntervalMinutes
        updated_at = (Get-Date).ToUniversalTime().ToString("o")
        runner_path = $runnerPath
        paper_only = $true
        live_execution_enabled = $false
    } | ConvertTo-Json -Depth 6 |
        Set-Content -LiteralPath $temporaryPath -Encoding utf8
    Move-Item -LiteralPath $temporaryPath -Destination $schedulePath -Force
}

function Show-ChainseerPonsTaskStatus {
    $task = Get-ChainseerPonsTask
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
        Action = $task.Actions[0].Execute
        Arguments = $task.Actions[0].Arguments
    } | Format-List
}

switch ($Command) {
    "install" {
        if (-not (Test-Path -LiteralPath $runnerPath -PathType Leaf)) {
            throw "Pons learning runner was not found: $runnerPath"
        }

        $existing = Get-ChainseerPonsTask
        if ($null -ne $existing) {
            $existingArguments = [string]$existing.Actions[0].Arguments
            if (-not $existingArguments.Contains($runnerPath)) {
                throw "Task '$taskName' exists but points elsewhere; refusing to replace it."
            }
        }

        $userId = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
        $arguments = '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "{0}"' -f $runnerPath
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
            -ExecutionTimeLimit (New-TimeSpan -Minutes 5) `
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
            -Description "Paper-only bounded Pons discovery, one full-risk position refresh, managed/shadow entry evaluation, and Timechain learning; the separate fast guard owns frequent quote marks."

        Register-ScheduledTask -TaskName $taskName -InputObject $definition -Force |
            Out-Null
        Write-ScheduleState -Installed $true -Enabled $true
        Write-Output "Installed '$taskName'. It runs every $IntervalMinutes minutes while $userId is logged on."
        Show-ChainseerPonsTaskStatus
    }
    "enable" {
        if ($null -eq (Get-ChainseerPonsTask)) {
            throw "Task '$taskName' is not installed."
        }
        Enable-ScheduledTask -TaskName $taskName | Out-Null
        Write-ScheduleState -Installed $true -Enabled $true
        Show-ChainseerPonsTaskStatus
    }
    "disable" {
        if ($null -ne (Get-ChainseerPonsTask)) {
            Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
            Disable-ScheduledTask -TaskName $taskName | Out-Null
        }
        Write-ScheduleState `
            -Installed ($null -ne (Get-ChainseerPonsTask)) `
            -Enabled $false
        Show-ChainseerPonsTaskStatus
    }
    "status" {
        Show-ChainseerPonsTaskStatus
    }
    "run-now" {
        if ($null -eq (Get-ChainseerPonsTask)) {
            throw "Task '$taskName' is not installed."
        }
        Start-ScheduledTask -TaskName $taskName
        Write-Output "Started '$taskName'. Use status or the dashboard to inspect it."
    }
    "uninstall" {
        if ($null -eq (Get-ChainseerPonsTask)) {
            Write-Output "Task '$taskName' is already absent."
            Write-ScheduleState -Installed $false -Enabled $false
            break
        }
        Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
        Write-ScheduleState -Installed $false -Enabled $false
        Write-Output "Removed '$taskName'. Pons runtime data, ledgers, Timechain, and logs were preserved."
    }
}
