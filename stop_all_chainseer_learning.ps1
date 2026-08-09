# Hard stop for an unattended learning window.
#
# Disable-ScheduledTask alone does NOT stop an instance that is already
# running -- a learn cycle can take over 5 minutes, so disabling mid-cycle
# would leave a live writer behind a stale lock. Disable first so nothing new
# starts, then stop whatever is still in flight.
$ErrorActionPreference = "Stop"

$tasks = @(
    "Chainseer Base Learn Once",
    "Chainseer Pons Learn Once",
    "Chainseer Pons Fast Guard",
    "Chainseer Solana Learn Once"
)

$stamp = (Get-Date).ToUniversalTime().ToString("o")
$logPath = Join-Path $PSScriptRoot "learning_window_stop.log"
"[$stamp] STOP requested" | Out-File -LiteralPath $logPath -Append -Encoding utf8

foreach ($name in $tasks) {
    try {
        Disable-ScheduledTask -TaskName $name -ErrorAction Stop | Out-Null
        "[$stamp] disabled $name" | Out-File -LiteralPath $logPath -Append -Encoding utf8
    } catch {
        "[$stamp] FAILED to disable ${name}: $_" |
            Out-File -LiteralPath $logPath -Append -Encoding utf8
    }
}

# Give an in-flight cycle a chance to finish on its own before forcing it.
# The longest measured cycle is ~330s (Pons), so wait a little beyond that.
$deadline = (Get-Date).AddSeconds(420)
while ((Get-Date) -lt $deadline) {
    $running = @($tasks | ForEach-Object {
        try { Get-ScheduledTask -TaskName $_ -ErrorAction Stop } catch { $null }
    } | Where-Object { $_ -and $_.State -eq "Running" })
    if ($running.Count -eq 0) { break }
    Start-Sleep -Seconds 15
}

foreach ($name in $tasks) {
    try {
        $t = Get-ScheduledTask -TaskName $name -ErrorAction Stop
        if ($t.State -eq "Running") {
            Stop-ScheduledTask -TaskName $name -ErrorAction Stop
            "[$stamp] force-stopped $name (did not finish in time)" |
                Out-File -LiteralPath $logPath -Append -Encoding utf8
        }
    } catch {
        "[$stamp] could not check ${name}: $_" |
            Out-File -LiteralPath $logPath -Append -Encoding utf8
    }
}

$done = (Get-Date).ToUniversalTime().ToString("o")
"[$done] STOP complete" | Out-File -LiteralPath $logPath -Append -Encoding utf8
