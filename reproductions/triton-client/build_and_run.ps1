param(
    [string]$UpstreamRepo = "",
    [switch]$KeepBuild
)

$ErrorActionPreference = "Stop"
$PinnedTritonCommit = "13095d26b544a814d2b07f923d52e3d12eb859fa"
$env:PATH = "C:\msys64\ucrt64\bin;$env:PATH"

$ReproDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProtoDir = Join-Path $ReproDir "protos"
$BuildDir = Join-Path $env:TEMP ("triton_client_repro_" + [System.Guid]::NewGuid().ToString("N").Substring(0, 8))
$GenDir = Join-Path $BuildDir "gen"
$PatchedDir = Join-Path $BuildDir "patched_lib"
New-Item -ItemType Directory -Force -Path $GenDir, $PatchedDir | Out-Null

if ([string]::IsNullOrWhiteSpace($UpstreamRepo)) {
    $UpstreamRepo = Join-Path $BuildDir "upstream-triton-client"
    New-Item -ItemType Directory -Force -Path $UpstreamRepo | Out-Null
    & git -C $UpstreamRepo init | Out-Null
    & git -C $UpstreamRepo remote add origin "https://github.com/triton-inference-server/client.git"
    & git -C $UpstreamRepo sparse-checkout init --cone
    & git -C $UpstreamRepo sparse-checkout set "src/c++/library"
    & git -C $UpstreamRepo fetch --depth=1 --filter=blob:none origin $PinnedTritonCommit
    if ($LASTEXITCODE -ne 0) { throw "git fetch failed for triton-inference-server/client @ $PinnedTritonCommit" }
    & git -C $UpstreamRepo checkout --detach FETCH_HEAD
    if ($LASTEXITCODE -ne 0) { throw "git checkout failed for triton-inference-server/client @ $PinnedTritonCommit" }
}
$UpstreamLib = Join-Path $UpstreamRepo "src\c++\library"
if (-not (Test-Path (Join-Path $UpstreamLib "grpc_client.cc"))) {
    throw "Cannot find upstream Triton grpc_client.cc in $UpstreamLib"
}

$OutputLog = New-Object System.Collections.Generic.List[string]
function Log-Line([string]$Line) {
    Write-Host $Line
    $OutputLog.Add($Line)
}

try {
    Log-Line "=== Environment ==="
    Log-Line ("g++: " + ((& g++ --version | Select-Object -First 1)))
    Log-Line ("protoc: " + (& protoc.exe --version))
    $grpcPc = Get-Content "C:\msys64\ucrt64\lib\pkgconfig\grpc++.pc" | Select-String "Version:"
    Log-Line ("grpc++: " + $grpcPc.Line.Trim())
    Log-Line ("Upstream commit: " + (& git -C $UpstreamRepo rev-parse --short HEAD))
    Log-Line ""

    # 1. Generate protobuf + gRPC C++ sources from official Triton schemas
    & protoc.exe "-I$ProtoDir" "--cpp_out=$GenDir" "--grpc_out=$GenDir" `
        "--plugin=protoc-gen-grpc=C:\msys64\ucrt64\bin\grpc_cpp_plugin.exe" `
        "$ProtoDir\model_config.proto" "$ProtoDir\grpc_service.proto"
    if ($LASTEXITCODE -ne 0) { throw "protoc failed" }

    $AbslLibs = @(Get-ChildItem "C:\msys64\ucrt64\lib\libabsl_*.dll.a" | ForEach-Object { "-l" + ($_.Name -replace '^lib(.*)\.dll\.a$', '$1') })

    function Build-ProtoObjects {
        & g++ -std=c++17 -O0 -g -DPROTOBUF_USE_DLLS "-I$GenDir" "-IC:\msys64\ucrt64\include" -c "$GenDir\model_config.pb.cc" -o "$GenDir\model_config.pb.o"
        if ($LASTEXITCODE -ne 0) { throw "model_config.pb.cc failed" }
        & g++ -std=c++17 -O0 -g -DPROTOBUF_USE_DLLS "-I$GenDir" "-IC:\msys64\ucrt64\include" -c "$GenDir\grpc_service.pb.cc" -o "$GenDir\grpc_service.pb.o"
        if ($LASTEXITCODE -ne 0) { throw "grpc_service.pb.cc failed" }
        & g++ -std=c++17 -O0 -g -DPROTOBUF_USE_DLLS "-I$GenDir" "-IC:\msys64\ucrt64\include" -c "$GenDir\grpc_service.grpc.pb.cc" -o "$GenDir\grpc_service.grpc.pb.o"
        if ($LASTEXITCODE -ne 0) { throw "grpc_service.grpc.pb.cc failed" }
    }

    function Build-ClientBinary([string]$LibSrcDir, [string]$OutExe) {
        & g++ -std=c++17 -O0 -g -DPROTOBUF_USE_DLLS "-I$LibSrcDir" "-I$GenDir" "-IC:\msys64\ucrt64\include" -c "$LibSrcDir\common.cc" -o "$BuildDir\common.o"
        if ($LASTEXITCODE -ne 0) { throw "common.cc failed" }
        & g++ -std=c++17 -O0 -g -DPROTOBUF_USE_DLLS "-I$LibSrcDir" "-I$GenDir" "-IC:\msys64\ucrt64\include" -c "$LibSrcDir\grpc_client.cc" -o "$BuildDir\grpc_client.o"
        if ($LASTEXITCODE -ne 0) { throw "grpc_client.cc failed" }
        & g++ -std=c++17 -O0 -g -DPROTOBUF_USE_DLLS "-I$LibSrcDir" "-I$GenDir" "-IC:\msys64\ucrt64\include" `
            "$ReproDir\triton_repro_test.cpp" `
            "$BuildDir\grpc_client.o" "$BuildDir\common.o" `
            "$GenDir\grpc_service.grpc.pb.o" "$GenDir\grpc_service.pb.o" "$GenDir\model_config.pb.o" `
            "-LC:\msys64\ucrt64\lib" `
            "-Wl,--start-group" -lgrpc++ -lgrpc -lgpr -lprotobuf @AbslLibs "-Wl,--end-group" `
            -o $OutExe
        if ($LASTEXITCODE -ne 0) { throw "link failed for $OutExe" }
    }

    function Run-Probe([string]$ExePath, [string]$Mode) {
        $StdoutFile = Join-Path $BuildDir "${Mode}_stdout.txt"
        $StderrFile = Join-Path $BuildDir "${Mode}_stderr.txt"
        $proc = Start-Process -FilePath $ExePath -ArgumentList $Mode -NoNewWindow -Wait -PassThru `
            -RedirectStandardOutput $StdoutFile -RedirectStandardError $StderrFile
        Log-Line ("--- Mode: $Mode | ExitCode: {0} (0x{0:X8}) ---" -f $proc.ExitCode)
        if (Test-Path $StdoutFile) {
            foreach ($l in (Get-Content $StdoutFile)) { Log-Line ("  stdout: " + $l) }
        }
        if (Test-Path $StderrFile) {
            foreach ($l in (Get-Content $StderrFile)) {
                Log-Line ("  stderr: " + $l)
                if ($l -match 'FaultRVA=(0x[0-9a-fA-F]+)') {
                    $resolved = & addr2line.exe -e $ExePath -f -C -p $Matches[1]
                    Log-Line ("  addr2line(FaultRVA): " + $resolved)
                }
                if ($l -match 'StackRVAs:(.*)$') {
                    $rvas = ($Matches[1].Trim() -split '\s+') | Where-Object { $_ -match '^0x14[0-9a-fA-F]+$' }
                    foreach ($rva in ($rvas | Select-Object -First 8)) {
                        $resolved = & addr2line.exe -e $ExePath -f -C -p $rva
                        Log-Line ("  addr2line(frame $rva): " + $resolved)
                    }
                }
            }
        }
    }

    Build-ProtoObjects

    Log-Line "=== PART A: UNMODIFIED UPSTREAM TRITON CLIENT ==="
    $UnpatchedExe = Join-Path $BuildDir "triton_unpatched.exe"
    Build-ClientBinary -LibSrcDir $UpstreamLib -OutExe $UnpatchedExe

    foreach ($m in @("0_baseline", "1_guards", "2a_zero", "2a_aa", "2b_after_stop", "2b_cancel_after_stop", "2d_inflight_cancel")) {
        Run-Probe -ExePath $UnpatchedExe -Mode $m
    }

    Log-Line ""
    Log-Line "=== PART B: TEST 2C FIXED VERSION (3-HUNK PATCH) ==="
    Copy-Item "$UpstreamLib\*" $PatchedDir -Force

    # Apply Test 2C fix to temporary copy in $PatchedDir:
    # 1. grpc_client.h: bool enable_stream_stats_{false};
    $HdrPath = Join-Path $PatchedDir "grpc_client.h"
    $HdrText = [System.IO.File]::ReadAllText($HdrPath)
    $HdrTarget = "  bool enable_stream_stats_;"
    $HdrRepl   = "  bool enable_stream_stats_{false};"
    if (-not $HdrText.Contains($HdrTarget)) { throw "Header target not found" }
    [System.IO.File]::WriteAllText($HdrPath, $HdrText.Replace($HdrTarget, $HdrRepl))

    # 2. grpc_client.cc: grpc_stream_.reset() + enable_stream_stats_ = false in StopStream(),
    #    and guard in AsyncStreamInfer()
    $CcPath = Join-Path $PatchedDir "grpc_client.cc"
    $CcText = [System.IO.File]::ReadAllText($CcPath)
    $StopTarget = "    stream_worker_.join();`r`n    stream_cancel_requested_.store(false, std::memory_order_release);"
    $StopRepl   = "    stream_worker_.join();`r`n    grpc_stream_.reset();`r`n    enable_stream_stats_ = false;`r`n    stream_cancel_requested_.store(false, std::memory_order_release);"
    if (-not $CcText.Contains($StopTarget)) { throw "StopStream target not found" }
    $CcText = $CcText.Replace($StopTarget, $StopRepl)

    $InferTarget = "InferenceServerGrpcClient::AsyncStreamInfer(`r`n    const InferOptions& options, const std::vector<InferInput*>& inputs,`r`n    const std::vector<const InferRequestedOutput*>& outputs)`r`n{"
    $InferRepl   = "InferenceServerGrpcClient::AsyncStreamInfer(`r`n    const InferOptions& options, const std::vector<InferInput*>& inputs,`r`n    const std::vector<const InferRequestedOutput*>& outputs)`r`n{`r`n  if (!grpc_stream_ || !stream_worker_.joinable()) {`r`n    return Error(`r`n        `"Stream is not active; call StartStream() before AsyncStreamInfer().`");`r`n  }"
    if (-not $CcText.Contains($InferTarget)) { throw "AsyncStreamInfer target not found" }
    $CcText = $CcText.Replace($InferTarget, $InferRepl)
    [System.IO.File]::WriteAllText($CcPath, $CcText)

    # Save exact unified diff to triton_client_fix.patch with clean repo-relative headers
    $PatchPath = Join-Path $ReproDir "triton_client_fix.patch"
    Push-Location $UpstreamRepo
    try {
        $DiffH = (& git diff --no-index -- "src/c++/library/grpc_client.h" "$PatchedDir\grpc_client.h") `
            -replace '^diff --git .*', 'diff --git a/src/c++/library/grpc_client.h b/src/c++/library/grpc_client.h' `
            -replace '^--- .*', '--- a/src/c++/library/grpc_client.h' `
            -replace '^\+\+\+ .*', '+++ b/src/c++/library/grpc_client.h'
        $DiffC = (& git diff --no-index -- "src/c++/library/grpc_client.cc" "$PatchedDir\grpc_client.cc") `
            -replace '^diff --git .*', 'diff --git a/src/c++/library/grpc_client.cc b/src/c++/library/grpc_client.cc' `
            -replace '^--- .*', '--- a/src/c++/library/grpc_client.cc' `
            -replace '^\+\+\+ .*', '+++ b/src/c++/library/grpc_client.cc'
        @($DiffH; $DiffC) | Set-Content -Path $PatchPath -Encoding UTF8
    }
    finally {
        Pop-Location
    }

    $FixedExe = Join-Path $BuildDir "triton_fixed.exe"
    Build-ClientBinary -LibSrcDir $PatchedDir -OutExe $FixedExe

    foreach ($m in @("0_baseline", "1_guards", "2a_zero", "2a_aa", "2b_after_stop", "2b_cancel_after_stop", "2d_inflight_cancel")) {
        Run-Probe -ExePath $FixedExe -Mode $m
    }

    Log-Line ""
    Log-Line "=== PART C: HARDENED FIX (3-HUNK PATCH + TIMER QUEUE CLEAR IN StopStream) ==="
    $HardenedDir = Join-Path $BuildDir "hardened_lib"
    New-Item -ItemType Directory -Force -Path $HardenedDir | Out-Null
    Copy-Item "$PatchedDir\*" $HardenedDir -Force
    $HardCcPath = Join-Path $HardenedDir "grpc_client.cc"
    $HardCcText = [System.IO.File]::ReadAllText($HardCcPath)
    $DrainTarget = "    stream_worker_.join();`r`n    grpc_stream_.reset();`r`n    enable_stream_stats_ = false;"
    $DrainRepl   = "    stream_worker_.join();`r`n    grpc_stream_.reset();`r`n    enable_stream_stats_ = false;`r`n    {`r`n      std::lock_guard<std::mutex> lock(stream_mutex_);`r`n      while (!ongoing_stream_request_timers_.empty()) {`r`n        ongoing_stream_request_timers_.pop();`r`n      }`r`n    }"
    if (-not $HardCcText.Contains($DrainTarget)) { throw "Hardened StopStream target not found" }
    [System.IO.File]::WriteAllText($HardCcPath, $HardCcText.Replace($DrainTarget, $DrainRepl))

    $HardenedExe = Join-Path $BuildDir "triton_hardened.exe"
    Build-ClientBinary -LibSrcDir $HardenedDir -OutExe $HardenedExe
    foreach ($m in @("0_baseline", "1_guards", "2a_zero", "2a_aa", "2b_after_stop", "2b_cancel_after_stop", "2d_inflight_cancel")) {
        Run-Probe -ExePath $HardenedExe -Mode $m
    }

    $OutputPath = Join-Path $ReproDir "TEST_OUTPUT.txt"
    $OutputLog | Set-Content -Path $OutputPath -Encoding UTF8
    Write-Host "Wrote $OutputPath and $PatchPath"
}
finally {
    if (-not $KeepBuild -and (Test-Path $BuildDir)) {
        Remove-Item -Recurse -Force $BuildDir
    }
}
