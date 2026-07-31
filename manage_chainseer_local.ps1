param(
    [ValidateSet("start", "status", "stop")]
    [string]$Action = "start"
)

$ErrorActionPreference = "Stop"
$workspace = $PSScriptRoot
$webRoot = Join-Path $workspace "chainseer_web"
$statePath = Join-Path $workspace ".chainseer-local.json"
$apiOut = Join-Path $workspace "chainseer-local-api.stdout.log"
$apiErr = Join-Path $workspace "chainseer-local-api.stderr.log"
$webOut = Join-Path $webRoot ".dev-stdout.log"
$webErr = Join-Path $webRoot ".dev-stderr.log"
$webEnvPath = Join-Path $webRoot ".env.local"

function Get-LocalState {
    if (-not (Test-Path -LiteralPath $statePath)) {
        return $null
    }
    try {
        return Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
    }
    catch {
        return $null
    }
}

function Test-ExpectedProcess {
    param([int]$ProcessId, [string]$Marker)
    if ($ProcessId -le 0) {
        return $false
    }
    $process = Get-CimInstance Win32_Process -Filter "ProcessId = $ProcessId" -ErrorAction SilentlyContinue
    return (
        $null -ne $process -and
        $process.CommandLine -like "*$Marker*"
    )
}

function Stop-ExpectedProcess {
    param([int]$ProcessId, [string]$Marker)
    if (Test-ExpectedProcess -ProcessId $ProcessId -Marker $Marker) {
        Stop-Process -Id $ProcessId
    }
}

function Resolve-Python {
    $command = Get-Command python.exe -ErrorAction SilentlyContinue
    if ($command -and $command.Source -notlike "*WindowsApps*") {
        return $command.Source
    }
    $candidates = Get-ChildItem -Path (Join-Path $env:LOCALAPPDATA "Python") `
        -Recurse -Filter python.exe -ErrorAction SilentlyContinue |
        Where-Object { $_.FullName -like "*pythoncore-*-64*" } |
        Sort-Object FullName -Descending
    if ($candidates) {
        return $candidates[0].FullName
    }
    throw "Python 3.14 could not be found."
}

function Resolve-Node {
    $bundled = Join-Path $env:USERPROFILE ".cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe"
    if (Test-Path -LiteralPath $bundled) {
        return $bundled
    }
    $command = Get-Command node.exe -ErrorAction SilentlyContinue
    if ($command) {
        return $command.Source
    }
    throw "Node.js could not be found."
}

function Wait-Endpoint {
    param([string]$Uri, [int]$Seconds = 25)
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri $Uri -TimeoutSec 3
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) {
                return $true
            }
        }
        catch {
            Start-Sleep -Milliseconds 500
        }
    }
    return $false
}

$state = Get-LocalState

if ($Action -eq "status") {
    $apiRunning = $state -and (Test-ExpectedProcess -ProcessId $state.api_pid -Marker "chainseer_api.py")
    $webRunning = $state -and (Test-ExpectedProcess -ProcessId $state.web_pid -Marker "vinext")
    Write-Output "API:  $(if ($apiRunning) { 'RUNNING' } else { 'STOPPED' })"
    Write-Output "Web:  $(if ($webRunning) { 'RUNNING' } else { 'STOPPED' })"
    if ($webRunning) {
        Write-Output "Open: http://localhost:3000/"
    }
    exit 0
}

if ($Action -eq "stop") {
    if ($state) {
        Stop-ExpectedProcess -ProcessId $state.web_pid -Marker "vinext"
        Stop-ExpectedProcess -ProcessId $state.api_pid -Marker "chainseer_api.py"
    }
    Remove-Item -LiteralPath $statePath -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $webEnvPath -Force -ErrorAction SilentlyContinue
    Write-Output "Chainseer local review stopped."
    exit 0
}

if ($state) {
    $apiRunning = Test-ExpectedProcess -ProcessId $state.api_pid -Marker "chainseer_api.py"
    $webRunning = Test-ExpectedProcess -ProcessId $state.web_pid -Marker "vinext"
    if ($apiRunning -and $webRunning) {
        Write-Output "Chainseer local review is already running."
        Write-Output "Open: http://localhost:3000/"
        exit 0
    }
    Stop-ExpectedProcess -ProcessId $state.web_pid -Marker "vinext"
    Stop-ExpectedProcess -ProcessId $state.api_pid -Marker "chainseer_api.py"
    Remove-Item -LiteralPath $statePath -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $webEnvPath -Force -ErrorAction SilentlyContinue
}

$python = Resolve-Python
$node = Resolve-Node
$skillDir = Join-Path $env:USERPROFILE ".codex\skills\cypher-tempre-self-model"
if (-not (Test-Path -LiteralPath (Join-Path $skillDir "timechain.py"))) {
    throw "Cypher Tempre Timechain skill was not found at $skillDir"
}

$tokenBytes = New-Object byte[] 32
$tokenGenerator = [Security.Cryptography.RandomNumberGenerator]::Create()
$tokenGenerator.GetBytes($tokenBytes)
$tokenGenerator.Dispose()
$localToken = ([BitConverter]::ToString($tokenBytes) -replace "-", "").ToLowerInvariant()

$env:PYTHONUTF8 = "1"
$env:CHAINSEER_ENVIRONMENT = "development"
$env:CHAINSEER_API_HOST = "127.0.0.1"
$env:CHAINSEER_API_PORT = "8000"
$env:CHAINSEER_API_TOKEN = $localToken
$env:CHAINSEER_CHAIN_ROOT = Join-Path $workspace "chainseer_chain"
$env:CHAINSEER_SKILL_DIR = $skillDir
$env:CHAINSEER_ALLOWED_HOSTS = "127.0.0.1,localhost"
$env:CHAINSEER_ALLOWED_ORIGINS = "http://localhost:3000"

$apiProcess = Start-Process -FilePath $python `
    -ArgumentList "-X", "utf8", "chainseer_api.py" `
    -WorkingDirectory $workspace `
    -RedirectStandardOutput $apiOut `
    -RedirectStandardError $apiErr `
    -WindowStyle Hidden `
    -PassThru

if (-not (Wait-Endpoint -Uri "http://127.0.0.1:8000/health/ready")) {
    Stop-ExpectedProcess -ProcessId $apiProcess.Id -Marker "chainseer_api.py"
    throw "Chainseer API did not become ready. See $apiErr"
}

$env:CHAINSEER_API_URL = "http://127.0.0.1:8000"
@(
    "CHAINSEER_API_URL=http://127.0.0.1:8000"
    "CHAINSEER_API_TOKEN=$localToken"
) | Set-Content -LiteralPath $webEnvPath -Encoding ascii

$webProcess = Start-Process -FilePath $node `
    -ArgumentList "node_modules\vinext\dist\cli.js", "dev" `
    -WorkingDirectory $webRoot `
    -RedirectStandardOutput $webOut `
    -RedirectStandardError $webErr `
    -WindowStyle Hidden `
    -PassThru

if (-not (Wait-Endpoint -Uri "http://localhost:3000/")) {
    Stop-ExpectedProcess -ProcessId $webProcess.Id -Marker "vinext"
    Stop-ExpectedProcess -ProcessId $apiProcess.Id -Marker "chainseer_api.py"
    Remove-Item -LiteralPath $webEnvPath -Force -ErrorAction SilentlyContinue
    throw "Chainseer website did not become ready. See $webErr"
}

[ordered]@{
    api_pid = $apiProcess.Id
    web_pid = $webProcess.Id
    started_at = [DateTime]::UtcNow.ToString("o")
} | ConvertTo-Json | Set-Content -LiteralPath $statePath -Encoding utf8

Write-Output "Chainseer local review is running."
Write-Output "Open: http://localhost:3000/"
Write-Output "API:  http://127.0.0.1:8000/health/ready"
