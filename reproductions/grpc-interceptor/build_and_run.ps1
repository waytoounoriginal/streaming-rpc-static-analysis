param(
    [string]$UpstreamGrpcDir = "",
    [string]$MsysRoot = "C:\msys64\ucrt64"
)

$ErrorActionPreference = "Stop"
$PinnedGrpcCommit = "30d65f9e86ca605e8cad30989e1e095c5fa4327e"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BuildDir = Join-Path $ScriptDir "build"
$OutputFile = Join-Path $ScriptDir "TEST_OUTPUT.txt"

$env:PATH = "$MsysRoot\bin;$env:PATH"

if (-not (Test-Path $BuildDir)) {
    New-Item -ItemType Directory -Path $BuildDir | Out-Null
}

if ([string]::IsNullOrWhiteSpace($UpstreamGrpcDir)) {
    $UpstreamGrpcDir = Join-Path $BuildDir "upstream-grpc"
    if (-not (Test-Path (Join-Path $UpstreamGrpcDir "examples\cpp\interceptors\caching_interceptor.h"))) {
        Write-Host "[0/4] Fetching pinned grpc/grpc commit $PinnedGrpcCommit into $UpstreamGrpcDir..."
        if (Test-Path $UpstreamGrpcDir) {
            Remove-Item -Recurse -Force $UpstreamGrpcDir
        }
        New-Item -ItemType Directory -Path $UpstreamGrpcDir | Out-Null
        & git -C $UpstreamGrpcDir init | Out-Null
        & git -C $UpstreamGrpcDir remote add origin "https://github.com/grpc/grpc.git"
        & git -C $UpstreamGrpcDir sparse-checkout init --cone
        & git -C $UpstreamGrpcDir sparse-checkout set "examples/protos" "examples/cpp/interceptors"
        & git -C $UpstreamGrpcDir fetch --depth=1 --filter=blob:none origin $PinnedGrpcCommit
        if ($LASTEXITCODE -ne 0) { throw "git fetch failed for grpc/grpc @ $PinnedGrpcCommit" }
        & git -C $UpstreamGrpcDir checkout --detach FETCH_HEAD
        if ($LASTEXITCODE -ne 0) { throw "git checkout failed for grpc/grpc @ $PinnedGrpcCommit" }
    }
}

$ProtoDir = Join-Path $UpstreamGrpcDir "examples\protos"
$ProtoFile = Join-Path $ProtoDir "keyvaluestore.proto"
$InterceptorDir = Join-Path $UpstreamGrpcDir "examples\cpp\interceptors"

if (-not (Test-Path $ProtoFile)) {
    throw "Cannot find upstream proto at $ProtoFile"
}
if (-not (Test-Path (Join-Path $InterceptorDir "caching_interceptor.h"))) {
    throw "Cannot find upstream caching_interceptor.h in $InterceptorDir"
}

Write-Host "[1/4] Generating protobuf and gRPC C++ sources from upstream keyvaluestore.proto..."
& "$MsysRoot\bin\protoc.exe" `
    --proto_path="$ProtoDir" `
    --cpp_out="$BuildDir" `
    --grpc_out="$BuildDir" `
    --plugin=protoc-gen-grpc="$MsysRoot\bin\grpc_cpp_plugin.exe" `
    "$ProtoFile"
if ($LASTEXITCODE -ne 0) { throw "protoc failed with exit code $LASTEXITCODE" }

$AbslLibs = Get-ChildItem "$MsysRoot\lib\libabsl_*.dll.a" | ForEach-Object {
    "-l" + ($_.Name -replace '^lib', '' -replace '\.dll\.a$', '')
}

$ExePath = Join-Path $BuildDir "repro_caching_interceptor.exe"
$SrcRepro = Join-Path $ScriptDir "repro_caching_interceptor.cc"
$SrcPb = Join-Path $BuildDir "keyvaluestore.pb.cc"
$SrcGrpcPb = Join-Path $BuildDir "keyvaluestore.grpc.pb.cc"

Write-Host "[2/4] Compiling repro_caching_interceptor.exe against unmodified upstream caching_interceptor.h..."
$GxxArgs = @(
    "-std=c++17", "-O2", "-Wall", "-Wextra",
    "-I$ScriptDir",
    "-I$BuildDir",
    "-I$InterceptorDir",
    "-I$MsysRoot\include",
    $SrcRepro, $SrcPb, $SrcGrpcPb,
    "-L$MsysRoot\lib",
    "-Wl,--start-group",
    "-lgrpc++", "-lgrpc", "-lgpr", "-lprotobuf"
) + $AbslLibs + @(
    "-Wl,--end-group",
    "-lws2_32",
    "-o", $ExePath
)

& "$MsysRoot\bin\g++.exe" @GxxArgs
if ($LASTEXITCODE -ne 0) { throw "g++ failed with exit code $LASTEXITCODE" }

Write-Host "[3/4] Running full test suite..."
$prevEap = $ErrorActionPreference
$ErrorActionPreference = "Continue"
$MainOut = (& $ExePath 2>&1 | ForEach-Object { "$_" }) -join "`n"
$MainExit = $LASTEXITCODE
$ErrorActionPreference = $prevEap
if ($MainExit -ne 0) { throw "Test binary exited with code $MainExit`n$MainOut" }

Write-Host "[4/4] Running Case A1 & B1 with GRPC_VERBOSITY=DEBUG, GRPC_TRACE=api,call_error..."
$env:GRPC_VERBOSITY = "DEBUG"
$env:GRPC_TRACE = "api,call_error"
try {
    $ErrorActionPreference = "Continue"
    $TraceOut = (& $ExePath --trace-only 2>&1 | ForEach-Object { "$_" }) -join "`n"
    $TraceExit = $LASTEXITCODE
    $ErrorActionPreference = $prevEap
    if ($TraceExit -ne 0) { throw "Traced run exited with code $TraceExit`n$TraceOut" }
} finally {
    $ErrorActionPreference = $prevEap
    Remove-Item Env:GRPC_VERBOSITY -ErrorAction SilentlyContinue
    Remove-Item Env:GRPC_TRACE -ErrorAction SilentlyContinue
}

$Header = @(
    "==============================================================================",
    "Runtime Verification Output: grpc/grpc CachingInterceptor (libgrpc++ 1.72.0)",
    "Upstream Commit: " + (git -C $UpstreamGrpcDir rev-parse HEAD),
    "Compiler: " + (& "$MsysRoot\bin\g++.exe" --version | Select-Object -First 1),
    "Protoc: " + (& "$MsysRoot\bin\protoc.exe" --version),
    "==============================================================================",
    ""
) -join "`n"

$FullReport = $Header + $MainOut + "`n==============================================================================`n" +
    "GRPC_VERBOSITY=DEBUG GRPC_TRACE=api,call_error (--trace-only)`n" +
    "==============================================================================`n" +
    $TraceOut

[System.IO.File]::WriteAllText($OutputFile, $FullReport, [System.Text.UTF8Encoding]::new($false))
Write-Host "Saved test output to $OutputFile"
Write-Output $FullReport
