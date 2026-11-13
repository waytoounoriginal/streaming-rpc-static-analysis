$ErrorActionPreference = "Stop"

function Invoke-NativeCapture {
    param([string]$Exe, [string[]]$ArgList)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $lines = (& $Exe @ArgList 2>&1) | ForEach-Object {
        if ($_ -is [System.Management.Automation.ErrorRecord]) {
            $_.Exception.Message
        } else {
            "$_"
        }
    }
    $code = $LASTEXITCODE
    $ErrorActionPreference = $prev
    return @{ Lines = $lines; ExitCode = $code }
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

$env:PATH = "C:\msys64\ucrt64\bin;$env:PATH"
$grpcPlugin = "C:\msys64\ucrt64\bin\grpc_cpp_plugin.exe"

Write-Host "=== 1. Generating protobuf + gRPC C++ stubs ==="
& protoc.exe --proto_path=. --cpp_out=. --grpc_out=. --plugin=protoc-gen-grpc="$grpcPlugin" buildbox_repro.proto
if ($LASTEXITCODE -ne 0) { throw "protoc failed with exit code $LASTEXITCODE" }

Write-Host "=== 2. Compiling repro_main.exe with g++ -std=c++17 ==="
$abslLibs = Get-ChildItem "C:\msys64\ucrt64\lib\libabsl_*.dll.a" | ForEach-Object {
    "-l" + ($_.Name -replace '^lib(.*)\.dll\.a$', '$1')
}

$compileArgs = @(
    "-std=c++17", "-O2", "-Wall", "-Wextra", "-DPROTOBUF_USE_DLLS",
    "-I.", "-IC:\msys64\ucrt64\include",
    "repro_main.cpp", "buildbox_repro.pb.cc", "buildbox_repro.grpc.pb.cc",
    "-o", "repro_main.exe",
    "-LC:\msys64\ucrt64\lib",
    "-Wl,--start-group",
    "-lgrpc++", "-lgrpc", "-lgpr", "-lprotobuf"
) + $abslLibs + @(
    "-Wl,--end-group",
    "-lws2_32", "-lpthread"
)

& g++ @compileArgs
if ($LASTEXITCODE -ne 0) { throw "g++ compile failed with exit code $LASTEXITCODE" }

Write-Host "=== 3. Running full reproduction suite (no trace) ==="
$outputLines = @()
$outputLines += "=== ENVIRONMENT ==="
$outputLines += "protoc: $((& protoc.exe --version) 2>&1)"
$outputLines += "g++: $((& g++ --version | Select-Object -First 1) 2>&1)"
$outputLines += ""
$outputLines += "=== FULL REPRODUCTION SUITE OUTPUT ==="

$env:GRPC_TRACE = ""
$env:GRPC_VERBOSITY = ""
$suiteRes = Invoke-NativeCapture -Exe ".\repro_main.exe" -ArgList @("all")
if ($suiteRes.ExitCode -ne 0) { throw "repro_main.exe all failed with exit code $($suiteRes.ExitCode)" }
$outputLines += $suiteRes.Lines

Write-Host "=== 4. Running GRPC_TRACE=call_error,api on bytestream & stagetree ==="
$outputLines += ""
$outputLines += "=== GRPC_TRACE=call_error,api (bytestream) ==="
$env:GRPC_TRACE = "call_error,api"
$env:GRPC_VERBOSITY = "INFO"
$traceBsRes = Invoke-NativeCapture -Exe ".\repro_main.exe" -ArgList @("bytestream")
if ($traceBsRes.ExitCode -ne 0) { throw "repro_main.exe bytestream failed with exit code $($traceBsRes.ExitCode)" }
$outputLines += $traceBsRes.Lines

$outputLines += ""
$outputLines += "=== GRPC_TRACE=call_error,api (stagetree) ==="
$traceStRes = Invoke-NativeCapture -Exe ".\repro_main.exe" -ArgList @("stagetree")
if ($traceStRes.ExitCode -ne 0) { throw "repro_main.exe stagetree failed with exit code $($traceStRes.ExitCode)" }
$traceSt = $traceStRes.Lines
$outputLines += ($traceSt | Select-Object -First 120)
if ($traceSt.Count -gt 120) {
    $outputLines += "... (truncated $($traceSt.Count - 120) repetitive trace lines; non-trace summary lines below) ..."
    $outputLines += ($traceSt | Where-Object { $_ -notmatch '^I\d|^D\d' })
}

$env:GRPC_TRACE = ""
$env:GRPC_VERBOSITY = ""

Write-Host "=== 5. Checking compiler warning behavior on discarded grpc::Status ==="
$snippet = @'
#include <grpcpp/grpcpp.h>
grpc::Status testOffset(bool bad) {
    if (bad) {
        grpc::Status(grpc::StatusCode::OUT_OF_RANGE, "`write_offset` is not valid");
    }
    return grpc::Status::OK;
}
'@
$tmpCpp = Join-Path $env:TEMP "check_status_warn.cpp"
$tmpObj = Join-Path $env:TEMP "check_status_warn.o"
Set-Content -Path $tmpCpp -Value $snippet -Encoding UTF8
$warnRes = Invoke-NativeCapture -Exe "g++" -ArgList @("-std=c++17", "-Wall", "-Wextra", "-IC:\msys64\ucrt64\include", "-c", $tmpCpp, "-o", $tmpObj)
$warnOut = $warnRes.Lines
Remove-Item -Force -ErrorAction SilentlyContinue $tmpCpp, $tmpObj

$outputLines += ""
$outputLines += "=== COMPILER DIAGNOSTIC CHECK (-Wall -Wextra on discarded grpc::Status) ==="
if ($warnOut.Count -eq 0) {
    $outputLines += "(0 warnings emitted by g++ -Wall -Wextra because grpc::Status lacks unconditional [[nodiscard]])"
} else {
    $outputLines += $warnOut
}

$outputLines | Set-Content -Path (Join-Path $scriptDir "TEST_OUTPUT.txt") -Encoding UTF8

Write-Host "=== 6. Cleaning compiled binary before commit ==="
Remove-Item -Force -ErrorAction SilentlyContinue "repro_main.exe", "*.o"

Write-Host "Done! Results written to TEST_OUTPUT.txt"
Get-Content (Join-Path $scriptDir "TEST_OUTPUT.txt")
