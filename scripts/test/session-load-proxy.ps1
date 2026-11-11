#requires -Version 5.1
<#
.SYNOPSIS
  ACP stdio proxy that fails `session/load` on purpose, for testing the rebuild path.

.DESCRIPTION
  Starts the real agent CLI with its stdin redirected (stdout/stderr inherit this process,
  so agent responses reach Ora unchanged), forwards newline-delimited JSON-RPC from this
  process's stdin to the CLI, and answers `session/load` locally with a JSON-RPC error.

  Every other method (`initialize`, `session/new`, `session/prompt`, ...) is forwarded as-is.

  Set -PassThrough or ORA_ACP_PROXY_PASS_THROUGH=1 to forward everything, including
  `session/load`; the same script can then be used to replay the success path.

.PARAMETER CliPath
  Path to the real agent CLI executable, e.g. C:\Program Files\CodeAgentCLI\codeagent.exe.

.PARAMETER PassThrough
  Forward `session/load` instead of answering it with an error.

.PARAMETER CliArgs
  Remaining arguments are passed to the real CLI.

.EXAMPLE
  pwsh -NoProfile -File session-load-proxy.ps1 -CliPath "C:\Program Files\CodeAgentCLI\codeagent.exe"

.EXAMPLE
  pwsh -NoProfile -File session-load-proxy.ps1 -PassThrough -CliPath "C:\Program Files\CodeAgentCLI\codeagent.exe"
#>
param(
    [Parameter(Mandatory = $true)][string]$CliPath,
    [switch]$PassThrough,
    [Parameter(ValueFromRemainingArguments = $true)][string[]]$CliArgs = @()
)

$ErrorActionPreference = 'Stop'

# Ora speaks UTF-8 JSON-RPC; keep both directions byte-stable on Windows.
# The console handles can be unavailable when stdio is redirected, so the settings are best effort.
$utf8 = [System.Text.UTF8Encoding]::new($false)
try { [Console]::InputEncoding = $utf8 } catch { }
try { [Console]::OutputEncoding = $utf8 } catch { }

$passthrough = $PassThrough -or ($env:ORA_ACP_PROXY_PASS_THROUGH -eq '1')

$startInfo = [System.Diagnostics.ProcessStartInfo]::new()
$startInfo.FileName = $CliPath
# Windows PowerShell 5.1 has no ArgumentList; quote each remaining argument for the
# command line so paths with spaces keep working on both 5.1 and 7.
$quotedArgs = foreach ($arg in $CliArgs) {
    if ($arg -match '[\s"]') { '"' + ($arg -replace '"', '\"') + '"' } else { $arg }
}
$startInfo.Arguments = ($quotedArgs -join ' ')
$startInfo.UseShellExecute = $false
# Only stdin is redirected: the CLI inherits this process's stdout/stderr, so its
# JSON-RPC responses go straight to Ora without an extra copy in the proxy.
$startInfo.RedirectStandardInput = $true
$startInfo.RedirectStandardOutput = $false
$startInfo.RedirectStandardError = $false

$child = [System.Diagnostics.Process]::Start($startInfo)
$child.StandardInput.NewLine = "`n"

try {
    while ($null -ne ($line = [Console]::In.ReadLine())) {
        if (-not $passthrough -and -not [string]::IsNullOrWhiteSpace($line)) {
            $message = $null
            try {
                $message = $line | ConvertFrom-Json -ErrorAction Stop
            } catch {
                # Not JSON or not a framed message: let the CLI see the original bytes.
            }
            if ($null -ne $message -and $message.method -eq 'session/load' -and $null -ne $message.id) {
                $response = [ordered]@{
                    jsonrpc = '2.0'
                    id      = $message.id
                    error   = [ordered]@{
                        code    = -32601
                        message = 'session/load is disabled by the test proxy'
                    }
                } | ConvertTo-Json -Compress -Depth 6
                [Console]::Out.Write($response + "`n")
                [Console]::Out.Flush()
                continue
            }
        }

        $child.StandardInput.WriteLine($line)
        $child.StandardInput.Flush()
    }
} finally {
    try { $child.StandardInput.Close() } catch { }
    $child.WaitForExit()
}

exit $child.ExitCode
