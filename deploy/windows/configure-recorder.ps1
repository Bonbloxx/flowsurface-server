[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^https://')]
    [string] $ServerUrl,

    [Parameter(Mandatory = $true)]
    [string] $TokenFile,

    [string] $StatePath = (Join-Path $env:APPDATA 'flowsurface\saved-state.json')
)

$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $TokenFile -PathType Leaf)) {
    throw "Recorder token file does not exist: $TokenFile"
}
if (-not (Test-Path -LiteralPath $StatePath -PathType Leaf)) {
    throw "FlowSurface state file does not exist: $StatePath"
}

$token = (Get-Content -LiteralPath $TokenFile -Raw).Trim()
if ([string]::IsNullOrWhiteSpace($token)) {
    throw 'Recorder token file was empty.'
}

if (-not ('FlowSurface.NativeCredential' -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;

namespace FlowSurface {
    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
    public struct NativeCredentialData {
        public UInt32 Flags;
        public UInt32 Type;
        public IntPtr TargetName;
        public IntPtr Comment;
        public System.Runtime.InteropServices.ComTypes.FILETIME LastWritten;
        public UInt32 CredentialBlobSize;
        public IntPtr CredentialBlob;
        public UInt32 Persist;
        public UInt32 AttributeCount;
        public IntPtr Attributes;
        public IntPtr TargetAlias;
        public IntPtr UserName;
    }

    public static class NativeCredential {
        [DllImport("advapi32.dll", EntryPoint = "CredWriteW", CharSet = CharSet.Unicode,
            SetLastError = true)]
        public static extern bool CredWrite(ref NativeCredentialData credential, UInt32 flags);
    }
}
'@
}

# Match keyring-rs on Windows: target is "<user>.<service>", the user is the
# recorder URL, and password text is stored as a UTF-16LE generic-credential blob.
$targetName = "$ServerUrl.flowsurface.server"
$blob = [Text.Encoding]::Unicode.GetBytes($token)
$credential = [FlowSurface.NativeCredentialData]::new()
$credential.Type = 1 # CRED_TYPE_GENERIC
$credential.Persist = 3 # CRED_PERSIST_ENTERPRISE
$credential.CredentialBlobSize = $blob.Length
$credential.TargetName = [Runtime.InteropServices.Marshal]::StringToCoTaskMemUni($targetName)
$credential.UserName = [Runtime.InteropServices.Marshal]::StringToCoTaskMemUni($ServerUrl)
$credential.Comment = [Runtime.InteropServices.Marshal]::StringToCoTaskMemUni('keyring v3.6.3')
$credential.CredentialBlob = [Runtime.InteropServices.Marshal]::AllocHGlobal($blob.Length)

try {
    [Runtime.InteropServices.Marshal]::Copy($blob, 0, $credential.CredentialBlob, $blob.Length)
    if (-not [FlowSurface.NativeCredential]::CredWrite([ref] $credential, 0)) {
        $code = [Runtime.InteropServices.Marshal]::GetLastWin32Error()
        throw "Windows Credential Manager rejected the recorder token (error $code)."
    }
}
finally {
    [Array]::Clear($blob, 0, $blob.Length)
    if ($credential.CredentialBlob -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::FreeHGlobal($credential.CredentialBlob)
    }
    foreach ($ptr in @($credential.TargetName, $credential.UserName, $credential.Comment)) {
        if ($ptr -ne [IntPtr]::Zero) {
            [Runtime.InteropServices.Marshal]::FreeCoTaskMem($ptr)
        }
    }
}

$raw = Get-Content -LiteralPath $StatePath -Raw
$state = $raw | ConvertFrom-Json -Depth 100
$oldUrl = [string] $state.network.server_url
$oldUrlJson = $oldUrl | ConvertTo-Json -Compress
$newUrlJson = $ServerUrl | ConvertTo-Json -Compress
$urlMatches = [regex]::Matches($raw, [regex]::Escape($oldUrlJson)).Count
if ($urlMatches -ne 1) {
    throw "Expected exactly one serialized server URL, found $urlMatches. State was not changed."
}

$modeJson = $state.network.trade_fetch_mode | ConvertTo-Json -Compress
$serverModeJson = '{"type":"Server"}'
$updated = $raw.Replace($oldUrlJson, $newUrlJson)
if ($state.network.trade_fetch_mode.type -ne 'Server') {
    $modeMatches = [regex]::Matches($updated, [regex]::Escape($modeJson)).Count
    if ($modeMatches -ne 1) {
        throw "Expected exactly one serialized trade-fetch mode, found $modeMatches. State was not changed."
    }
    $updated = $updated.Replace($modeJson, $serverModeJson)
}

$verified = $updated | ConvertFrom-Json -Depth 100
if ($verified.network.server_url -ne $ServerUrl -or
    $verified.network.trade_fetch_mode.type -ne 'Server') {
    throw 'Updated state did not pass the recorder configuration check.'
}

$timestamp = Get-Date -Format 'yyyyMMddTHHmmss'
$backupPath = "$StatePath.before-vps-recorder-$timestamp.bak"
$tempPath = "$StatePath.vps-recorder-$timestamp.tmp"
Copy-Item -LiteralPath $StatePath -Destination $backupPath
try {
    [IO.File]::WriteAllText($tempPath, $updated, [Text.UTF8Encoding]::new($false))
    Move-Item -LiteralPath $tempPath -Destination $StatePath -Force
}
finally {
    if (Test-Path -LiteralPath $tempPath) {
        Remove-Item -LiteralPath $tempPath -Force
    }
}

Write-Output "Configured FlowSurface Server mode for $ServerUrl"
Write-Output "State backup: $backupPath"
