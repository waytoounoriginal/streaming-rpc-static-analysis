param(
    [switch]$CleanBuildDir
)

$ErrorActionPreference = "Stop"

# Use MSYS2 UCRT64 toolchain
$env:PATH = "C:\msys64\ucrt64\bin;$env:PATH"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProtoDir = Join-Path $ScriptDir "proto"
$BuildDir = Join-Path $ScriptDir "build"
$OutputFile = Join-Path $ScriptDir "TEST_OUTPUT.txt"

New-Item -ItemType Directory -Force -Path $BuildDir | Out-Null

Write-Host "[1/4] Generating Flight.pb.{h,cc} and Flight.grpc.pb.{h,cc} from Flight.proto..."
$GrpcPlugin = "C:\msys64\ucrt64\bin\grpc_cpp_plugin.exe"
& protoc.exe `
    -I"$ProtoDir" `
    -I"C:\msys64\ucrt64\include" `
    --cpp_out="$BuildDir" `
    --grpc_out="$BuildDir" `
    --plugin=protoc-gen-grpc="$GrpcPlugin" `
    (Join-Path $ProtoDir "Flight.proto")
if ($LASTEXITCODE -ne 0) { throw "protoc failed with exit code $LASTEXITCODE" }

Write-Host "[2/4] Collecting Abseil import libraries from C:\msys64\ucrt64\lib..."
$AbslLibs = Get-ChildItem "C:\msys64\ucrt64\lib\libabsl_*.dll.a" | ForEach-Object {
    "-l" + ($_.Name -replace '^lib', '' -replace '\.dll\.a$', '')
}

Write-Host "[3/4] Compiling repro_arrow_flight.cpp against real libgrpc++ 1.72.0..."
$ExePath = Join-Path $BuildDir "repro_arrow_flight.exe"
$SrcRepro = Join-Path $ScriptDir "repro_arrow_flight.cpp"
$SrcPb = Join-Path $BuildDir "Flight.pb.cc"
$SrcGrpcPb = Join-Path $BuildDir "Flight.grpc.pb.cc"

$GxxArgs = @(
    "-std=c++17", "-O2", "-Wall", "-Wextra",
    "-DPROTOBUF_USE_DLLS",
    "-I$BuildDir",
    "-IC:\msys64\ucrt64\include",
    $SrcRepro, $SrcPb, $SrcGrpcPb,
    "-o", $ExePath,
    "-LC:\msys64\ucrt64\lib",
    "-Wl,--start-group",
    "-lgrpc++", "-lgrpc", "-lgpr", "-lprotobuf"
) + $AbslLibs + @(
    "-Wl,--end-group",
    "-lws2_32", "-lpthread"
)

& g++ @GxxArgs
if ($LASTEXITCODE -ne 0) { throw "g++ failed with exit code $LASTEXITCODE" }

Write-Host "[4/4] Running loopback verification binary (normal + GRPC_TRACE=api,call_error)..."
$ErrorActionPreference = "Continue"
$NormalOutput = & cmd /c "`"$ExePath`" 2>&1" | Out-String
$NormalExit = $LASTEXITCODE
if ($NormalExit -ne 0) {
    Write-Host $NormalOutput
    throw "repro_arrow_flight.exe failed with exit code $NormalExit"
}

$PrevTrace = $env:GRPC_TRACE
$PrevVerb = $env:GRPC_VERBOSITY
$env:GRPC_TRACE = "api,call_error"
$env:GRPC_VERBOSITY = "INFO"
try {
    $TraceOutput = & cmd /c "`"$ExePath`" 2>&1" | Out-String
    $TraceExit = $LASTEXITCODE
    if ($TraceExit -ne 0) {
        Write-Host $TraceOutput
        throw "repro_arrow_flight.exe (trace run) failed with exit code $TraceExit"
    }
} finally {
    $env:GRPC_TRACE = $PrevTrace
    $env:GRPC_VERBOSITY = $PrevVerb
}
$ErrorActionPreference = "Stop"

$TraceLines = $TraceOutput -split "`r?`n"
$TargetedTrace = $TraceLines | Where-Object {
    $_ -match '--- \[|\[Interceptor\]|RECV_STATUS_ON_CLIENT|grpc_call_cancel|CancelWithError|RST_STREAM|SEND_STATUS_FROM_SERVER|call_error|Attempt to send'
}

$Header = @"
# Execution captured by build_and_run.ps1
# Toolchain: $((& g++ --version | Select-Object -First 1))
# Protoc:    $((& protoc --version))
# Timestamp: $(Get-Date -Format "yyyy-MM-ddTHH:mm:ssK")

=== SECTION 1: CLEAN LOOPBACK EXECUTION ===
$NormalOutput
=== SECTION 2: GRPC_TRACE=api,call_error GRPC_VERBOSITY=INFO EXCERPT (FIRST 160 LINES) ===
$(($TraceLines | Select-Object -First 160) -join "`r`n")

=== SECTION 3: GRPC_TRACE TARGETED LIFECYCLE EVENTS (ALL CASES) ===
$($TargetedTrace -join "`r`n")
"@

Set-Content -Path $OutputFile -Value $Header -Encoding UTF8
Write-Host $NormalOutput
Write-Host "[Done] Saved output to $OutputFile"

if ($CleanBuildDir) {
    Remove-Item -Recurse -Force $BuildDir
    Write-Host "[Clean] Removed $BuildDir"
}
