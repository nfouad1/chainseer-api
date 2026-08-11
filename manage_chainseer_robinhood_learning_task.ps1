[CmdletBinding()]
param(
  [Parameter(Position=0)][ValidateSet("install","start","stop","status","run-now","uninstall")][string]$Command="status",
  [ValidateRange(1,1440)][int]$IntervalMinutes=5
)
$ErrorActionPreference="Stop"
$taskName="Chainseer Robinhood Paper Learning"
$runner=Join-Path $PSScriptRoot "run_chainseer_robinhood_learning.ps1"
$root=Join-Path $PSScriptRoot "robinhood_learning"
$schedule=Join-Path $root "schedule.json"
$powershell="$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"
function Get-Task { Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue }
function Write-State([bool]$installed,[bool]$enabled) {
  New-Item -ItemType Directory -Path $root -Force|Out-Null
  @{task_name=$taskName;installed=$installed;enabled=$enabled;interval_minutes=$IntervalMinutes;updated_at=(Get-Date).ToUniversalTime().ToString("o");paper_only=$true;live_execution_enabled=$false}|ConvertTo-Json|Set-Content -LiteralPath "$schedule.tmp" -Encoding utf8
  Move-Item -LiteralPath "$schedule.tmp" -Destination $schedule -Force
}
function Install-Task {
  if(-not(Test-Path -LiteralPath $runner)){throw "Runner not found: $runner"}
  $action=New-ScheduledTaskAction -Execute $powershell -Argument ('-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "{0}"' -f $runner) -WorkingDirectory $PSScriptRoot
  $trigger=New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes) -RepetitionDuration (New-TimeSpan -Days 3650)
  $settings=New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Minutes 20) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
  $principal=New-ScheduledTaskPrincipal -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Limited
  Register-ScheduledTask -TaskName $taskName -InputObject (New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description "Paper-only Robinhood Chain launch learning and outcome observation.") -Force|Out-Null
  Write-State $true $true
}
function Show-Status { $t=Get-Task;if($null-eq$t){"Task '$taskName' is not installed.";return};$i=Get-ScheduledTaskInfo -TaskName $taskName;[pscustomobject]@{TaskName=$t.TaskName;State=$t.State;LastRunTime=$i.LastRunTime;LastTaskResult=$i.LastTaskResult;NextRunTime=$i.NextRunTime;Enabled=$t.Settings.Enabled}|Format-List }
switch($Command){
 "install"{Install-Task;Start-ScheduledTask -TaskName $taskName;Show-Status}
 "start"{if($null-eq(Get-Task)){Install-Task};Enable-ScheduledTask -TaskName $taskName|Out-Null;Write-State $true $true;Start-ScheduledTask -TaskName $taskName;Show-Status}
 "stop"{if($null-ne(Get-Task)){Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue;Disable-ScheduledTask -TaskName $taskName|Out-Null};Write-State ($null-ne(Get-Task)) $false;Show-Status}
 "status"{Show-Status}
 "run-now"{if($null-eq(Get-Task)){throw "Task is not installed"};Start-ScheduledTask -TaskName $taskName;Show-Status}
 "uninstall"{if($null-ne(Get-Task)){Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue;Unregister-ScheduledTask -TaskName $taskName -Confirm:$false};Write-State $false $false;"Removed '$taskName'; learning data was preserved."}
}
