$ErrorActionPreference = "Stop"

$MinDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Ex1Dir = Join-Path $MinDir "01_pingpong_handshake"
$Ex2Dir = Join-Path $MinDir "02_config_stream"
$GenDir = Join-Path $MinDir "generated"
$BuildDir = Join-Path $MinDir "build"
$OutFile = Join-Path $MinDir "TEST_OUTPUT.txt"

New-Item -ItemType Directory -Force -Path $GenDir | Out-Null
New-Item -ItemType Directory -Force -Path $BuildDir | Out-Null

$PluginExe = (Get-Command grpc_cpp_plugin).Source

# 1. Generate Protobuf + gRPC C++ stubs
& protoc.exe -I "C:\msys64\ucrt64\include" -I "$Ex1Dir" --cpp_out="$GenDir" --grpc_out="$GenDir" --plugin="protoc-gen-grpc=$PluginExe" "$Ex1Dir\handshake.proto"
if ($LASTEXITCODE -ne 0) { throw "protoc failed on handshake.proto" }

& protoc.exe -I "C:\msys64\ucrt64\include" -I "$Ex2Dir" --cpp_out="$GenDir" --grpc_out="$GenDir" --plugin="protoc-gen-grpc=$PluginExe" "$Ex2Dir\config_stream.proto"
if ($LASTEXITCODE -ne 0) { throw "protoc failed on config_stream.proto" }

$Log = New-Object System.Collections.Generic.List[string]
$Log.Add("=== Minimal End-to-End Examples: Static + libgrpc++ 1.72.0 Runtime Verification ===")
$Log.Add("")

# 2. Run .proto -> dual .session.json + Clang AST static verifier across all 10 client/server files
$Log.Add("--- [PHASE 1] Session Compiler + ClangSessionChecker ---")
$StaticOut = (& python "$MinDir\verify_static.py" 2>&1) -join "`n"
$StaticCode = $LASTEXITCODE
$Log.Add($StaticOut)
if ($StaticCode -ne 0) {
    $Log | Set-Content -Path $OutFile -Encoding UTF8
    throw "Static verification failed (exit=$StaticCode)"
}

# 3. Compile & link real HTTP/2 loopback executables (libgrpc++ 1.72.0, g++ 15.2.0)
$abslLibs = @(
    Get-ChildItem "C:\msys64\ucrt64\lib\libabsl*.a" |
    ForEach-Object { "-l" + ($_.Name -replace '^lib', '' -replace '(\.dll)?\.a$', '') } |
    Sort-Object -Unique
)
$linkFlags = @("-L`"C:\msys64\ucrt64\lib`"", "-Wl,--start-group", "-lgrpc++", "-lgrpc", "-lgpr", "-lprotobuf") + $abslLibs + @("-Wl,--end-group", "-lws2_32")
$incFlags = @("-I`"$GenDir`"", "-I`"C:\msys64\ucrt64\include`"")

$Ex1Exe = Join-Path $BuildDir "ex1_loopback.exe"
& g++ -std=c++17 -O2 -Wall -Wextra @incFlags `
    "$GenDir\handshake.pb.cc" "$GenDir\handshake.grpc.pb.cc" `
    "$Ex1Dir\client_good.cpp" "$Ex1Dir\client_bad_swap.cpp" "$Ex1Dir\client_bad_leak.cpp" `
    "$Ex1Dir\server_good.cpp" "$Ex1Dir\server_bad_order.cpp" "$Ex1Dir\server_bad_missing_summary.cpp" `
    "$Ex1Dir\loopback_main.cpp" -o "$Ex1Exe" @linkFlags
if ($LASTEXITCODE -ne 0) { throw "Failed compiling ex1_loopback.exe" }

$Ex2Exe = Join-Path $BuildDir "ex2_loopback.exe"
& g++ -std=c++17 -O2 -Wall -Wextra @incFlags `
    "$GenDir\config_stream.pb.cc" "$GenDir\config_stream.grpc.pb.cc" `
    "$Ex2Dir\client_good.cpp" "$Ex2Dir\client_bad_oneof.cpp" "$Ex2Dir\client_bad_post_close.cpp" `
    "$Ex2Dir\server_good.cpp" "$Ex2Dir\loopback_main.cpp" -o "$Ex2Exe" @linkFlags
if ($LASTEXITCODE -ne 0) { throw "Failed compiling ex2_loopback.exe" }

# 4. Run real HTTP/2 loopback executables
$Log.Add("")
$Log.Add("--- [PHASE 2] Real libgrpc++ 1.72.0 HTTP/2 Loopback Runs ---")
$Ex1Out = (& "$Ex1Exe" 2>&1) -join "`n"
$Ex1Code = $LASTEXITCODE
$Log.Add("[Example 1: 01_pingpong_handshake] ExitCode=$Ex1Code")
$Log.Add($Ex1Out)

$Ex2Out = (& "$Ex2Exe" 2>&1) -join "`n"
$Ex2Code = $LASTEXITCODE
$Log.Add("")
$Log.Add("[Example 2: 02_config_stream] ExitCode=$Ex2Code")
$Log.Add($Ex2Out)

$cleanText = [regex]::Replace(($Log -join "`n"), "\x1B\[[0-9;]*[mK]", "")
$cleanText | Set-Content -Path $OutFile -Encoding UTF8
Write-Host ($Log -join "`n")

if ($Ex1Code -ne 0 -or $Ex2Code -ne 0) {
    throw "Loopback verification failed (Ex1=$Ex1Code, Ex2=$Ex2Code)"
}
