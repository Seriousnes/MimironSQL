[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $OutRoot
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

if ([string]::IsNullOrWhiteSpace($OutRoot)) {
    throw 'OutRoot is required.'
}

$OutRoot = [System.IO.Path]::GetFullPath($OutRoot)
New-Item -ItemType Directory -Force -Path $OutRoot | Out-Null

$manifestPath = Join-Path $OutRoot 'manifest.json'
$zipPath = Join-Path $OutRoot 'dbd.zip'
$markerPath = Join-Path $OutRoot '.mimironsql_wowdbdefs_ready'

# If already ready, no-op.
if ((Test-Path $markerPath) -and (Test-Path $manifestPath)) {
    return
}

$releaseApiUrl = 'https://api.github.com/repos/wowdev/WoWDBDefs/releases/latest'

$headers = @{
    'User-Agent' = 'MimironSQL-wowdbdefs-downloader'
    'Accept' = 'application/vnd.github+json'
    'X-GitHub-Api-Version' = '2022-11-28'
}

$token = $env:GITHUB_TOKEN
if (-not [string]::IsNullOrWhiteSpace($token)) {
    $headers['Authorization'] = "Bearer $token"
}

Write-Host "MimironSQL.DbContextGenerator: Fetching WoWDBDefs release metadata: $releaseApiUrl"
$release = Invoke-RestMethod -Uri $releaseApiUrl -Headers $headers -Method Get

if ($null -eq $release.assets) {
    throw 'WoWDBDefs GitHub response did not include assets.'
}

function Get-AssetUrl([object[]] $assets, [string] $name) {
    foreach ($a in $assets) {
        if ($null -eq $a) { continue }
        if ($a.name -and ($a.name -ieq $name)) {
            return $a.browser_download_url
        }
    }
    return $null
}

$manifestUrl = Get-AssetUrl -assets $release.assets -name 'manifest.json'
$dbdZipUrl = Get-AssetUrl -assets $release.assets -name 'dbd.zip'

if ([string]::IsNullOrWhiteSpace($manifestUrl)) { throw 'Could not find manifest.json in WoWDBDefs latest release assets.' }
if ([string]::IsNullOrWhiteSpace($dbdZipUrl)) { throw 'Could not find dbd.zip in WoWDBDefs latest release assets.' }

Write-Host "MimironSQL.DbContextGenerator: Downloading manifest.json -> $manifestPath"
Invoke-WebRequest -Uri $manifestUrl -Headers $headers -OutFile $manifestPath

Write-Host "MimironSQL.DbContextGenerator: Downloading dbd.zip -> $zipPath"
Invoke-WebRequest -Uri $dbdZipUrl -Headers $headers -OutFile $zipPath

Write-Host "MimironSQL.DbContextGenerator: Extracting dbd.zip -> $OutRoot"
Expand-Archive -Path $zipPath -DestinationPath $OutRoot -Force

try { Remove-Item -Force -ErrorAction SilentlyContinue $zipPath } catch { }

# Marker file indicates a successful extraction.
Set-Content -Path $markerPath -Value ([DateTimeOffset]::UtcNow.ToString('o')) -NoNewline
