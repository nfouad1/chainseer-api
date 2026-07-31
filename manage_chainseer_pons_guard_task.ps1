[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet("install", "enable", "disable", "status", "run-now", "uninstall")]
    [string]$Command = "status",

    [ValidateRange(1, 60)]
    [int]$IntervalMinutes = 2
)

$ErrorActionPreference = "Stop"
$taskName = "Chainseer Pons Fast Guard"
$runnerPath = Join-Path $PSScriptRoot "run_chainseer_pons_guard.ps1"
$learningRoot = Join-Path $PSScriptRoot "pons_learning"
$schedulePath = Join-Path $learningRoot "guard_schedule.json"
$powershellPath = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"

function Get-GuardTask {
    Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
}

function Write-ScheduleState {
    param([bool]$Installed, [bool]$Enabled)
    New-Item -ItemType Directory -Path $learningRoot -Force | Out-Null
    $temporaryPath = "$schedulePath.tmp"
    @{
        task_name = $taskName
        installed = $Installed
        enabled = $Enabled
        interval_minutes = $IntervalMinutes
        updated_at = (Get-Date).ToUniversalTime().ToString("o")
        runner_path = $runnerPath
        mode = "fast_executable_quote_guard"
        paper_only = $true
        live_execution_enabled = $false
    } | ConvertTo-Json -Depth 6 |
        Set-Content -LiteralPath $temporaryPath -Encoding utf8
    Move-Item -LiteralPath $temporaryPath -Destination $schedulePath -Force
}

function Show-GuardStatus {
    $task = Get-GuardTask
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
        MultipleInstances = $task.Settings.MultipleInstances
        ExecutionTimeLimit = $task.Settings.ExecutionTimeLimit
        Arguments = $task.Actions[0].Arguments
    } | Format-List
}

switch ($Command) {
    "install" {
        if (-not (Test-Path -LiteralPath $runnerPath -PathType Leaf)) {
            throw "Pons guard runner was not found: $runnerPath"
        }
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
            -ExecutionTimeLimit (New-TimeSpan -Minutes 2) `
            -AllowStartIfOnBatteries `
            -DontStopIfGoingOnBatteries
        $principal = New-ScheduledTaskPrincipal `
            -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) `
            -LogonType Interactive `
            -RunLevel Limited
        $definition = New-ScheduledTask `
            -Action $action `
            -Trigger $trigger `
            -Settings $settings `
            -Principal $principal `
            -Description "Paper-only fast executable-quote lifecycle guard for open Chainseer Pons positions."
        Register-ScheduledTask -TaskName $taskName -InputObject $definition -Force |
            Out-Null
        Write-ScheduleState -Installed $true -Enabled $true
        Show-GuardStatus
    }
    "enable" {
        Enable-ScheduledTask -TaskName $taskName | Out-Null
        Write-ScheduleState -Installed $true -Enabled $true
        Show-GuardStatus
    }
    "disable" {
        if ($null -ne (Get-GuardTask)) {
            Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
            Disable-ScheduledTask -TaskName $taskName | Out-Null
        }
        Write-ScheduleState -Installed ($null -ne (Get-GuardTask)) -Enabled $false
        Show-GuardStatus
    }
    "status" {
        Show-GuardStatus
    }
    "run-now" {
        Start-ScheduledTask -TaskName $taskName
        Write-Output "Started '$taskName'."
    }
    "uninstall" {
        if ($null -ne (Get-GuardTask)) {
            Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
        }
        Write-ScheduleState -Installed $false -Enabled $false
        Write-Output "Removed '$taskName'; all Pons state and logs were preserved."
    }
}
