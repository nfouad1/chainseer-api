[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet("install", "status", "run-now", "uninstall")]
    [string]$Command = "status"
)

$ErrorActionPreference = "Stop"
$taskName = "Chainseer Base Learn Once"
$runnerPath = Join-Path $PSScriptRoot "run_chainseer_learning.ps1"
$powershellPath = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"

function Get-ChainseerTask {
    Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
}

function Show-ChainseerTaskStatus {
    $task = Get-ChainseerTask
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
            throw "Learning runner was not found: $runnerPath"
        }

        $existing = Get-ChainseerTask
        if ($null -ne $existing) {
            $existingArguments = [string]$existing.Actions[0].Arguments
            if (-not $existingArguments.Contains($runnerPath)) {
                throw "Task '$taskName' exists but does not point to $runnerPath; refusing to replace it."
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
            -RepetitionInterval (New-TimeSpan -Minutes 2) `
            -RepetitionDuration (New-TimeSpan -Days 3650)
        $settings = New-ScheduledTaskSettingsSet `
            -MultipleInstances IgnoreNew `
            -StartWhenAvailable `
            -ExecutionTimeLimit (New-TimeSpan -Minutes 20) `
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
            -Description "Paper-only Chainseer Base observation and outcome-learning cycle."

        Register-ScheduledTask -TaskName $taskName -InputObject $definition -Force | Out-Null
        Write-Output "Installed '$taskName'. It runs every two minutes while $userId is logged on."
        Show-ChainseerTaskStatus
    }
    "status" {
        Show-ChainseerTaskStatus
    }
    "run-now" {
        if ($null -eq (Get-ChainseerTask)) {
            throw "Task '$taskName' is not installed."
        }
        Start-ScheduledTask -TaskName $taskName
        Write-Output "Started '$taskName'. Use the status command to inspect the result."
    }
    "uninstall" {
        if ($null -eq (Get-ChainseerTask)) {
            Write-Output "Task '$taskName' is already absent."
            break
        }
        Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
        Write-Output "Removed '$taskName'. Runtime data and logs were preserved."
    }
}
