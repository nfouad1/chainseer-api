[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$workspacePath = $PSScriptRoot
$pythonPath = "C:\Users\sonas\AppData\Local\Python\pythoncore-3.14-64\python.exe"
$chainseerPath = Join-Path $workspacePath "chainseer_base.py"
$learningRoot = Join-Path $workspacePath "base_learning"
$chainRoot = Join-Path $workspacePath "chainseer_chain"
$logRoot = Join-Path $learningRoot "logs"
$locationPushed = $false

if (-not (Test-Path -LiteralPath $pythonPath -PathType Leaf)) {
    throw "Chainseer Python runtime was not found: $pythonPath"
}
if (-not (Test-Path -LiteralPath $chainseerPath -PathType Leaf)) {
    throw "Chainseer entry point was not found: $chainseerPath"
}

New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
$logPath = Join-Path $logRoot ("learn-once-{0}.log" -f (Get-Date -Format "yyyy-MM-dd"))
$startedAt = (Get-Date).ToUniversalTime().ToString("o")

try {
    Push-Location -LiteralPath $workspacePath
    $locationPushed = $true

    "[$startedAt] START learn-once" | Out-File -LiteralPath $logPath -Append -Encoding utf8
    $output = @(
        & $pythonPath -X utf8 $chainseerPath learn-once `
            --limit 5 `
            --refresh-limit 3 `
            --amount-virtual 1 `
            --root $learningRoot `
            --chain-root $chainRoot 2>&1
    )
    $exitCode = $LASTEXITCODE

    if ($output.Count -gt 0) {
        $output | ForEach-Object { $_.ToString() } |
            Out-File -LiteralPath $logPath -Append -Encoding utf8
    }

    $finishedAt = (Get-Date).ToUniversalTime().ToString("o")
    "[$finishedAt] END learn-once exit_code=$exitCode" |
        Out-File -LiteralPath $logPath -Append -Encoding utf8

    if ($exitCode -ne 0) {
        throw "Chainseer learn-once exited with code $exitCode. See $logPath"
    }
}
catch {
    $failedAt = (Get-Date).ToUniversalTime().ToString("o")
    "[$failedAt] ERROR $($_.Exception.Message)" |
        Out-File -LiteralPath $logPath -Append -Encoding utf8
    Write-Error $_
    exit 1
}
finally {
    if ($locationPushed) {
        Pop-Location
    }
}

exit 0
