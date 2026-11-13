param(
    [string]$UpstreamWenetDir = ""
)

$ErrorActionPreference = "Stop"
$PinnedWenetCommit = "d17059667d6afe0680d19b3a4948ab825ef25105"

$ReproDir = $PSScriptRoot
$env:PATH = "C:\msys64\ucrt64\bin;$env:PATH"

$BuildDir = Join-Path $ReproDir "_build"
$GenDir = Join-Path $BuildDir "gen"
$UnpatchedDir = Join-Path $BuildDir "unpatched"
$PatchedDir = Join-Path $BuildDir "patched"
New-Item -ItemType Directory -Force -Path $GenDir, $UnpatchedDir, $PatchedDir | Out-Null

if ([string]::IsNullOrWhiteSpace($UpstreamWenetDir)) {
    $UpstreamWenetDir = Join-Path $BuildDir "upstream-wenet"
    if (-not (Test-Path (Join-Path $UpstreamWenetDir "runtime\core\grpc\grpc_server.cc"))) {
        Write-Host "[0/4] Fetching pinned wenet-e2e/wenet commit $PinnedWenetCommit into $UpstreamWenetDir..."
        if (Test-Path $UpstreamWenetDir) {
            Remove-Item -Recurse -Force $UpstreamWenetDir
        }
        New-Item -ItemType Directory -Force -Path $UpstreamWenetDir | Out-Null
        & git -C $UpstreamWenetDir init | Out-Null
        & git -C $UpstreamWenetDir remote add origin "https://github.com/wenet-e2e/wenet.git"
        & git -C $UpstreamWenetDir sparse-checkout init --cone
        & git -C $UpstreamWenetDir sparse-checkout set "runtime/core/grpc" "runtime/core/utils"
        & git -C $UpstreamWenetDir fetch --depth=1 --filter=blob:none origin $PinnedWenetCommit
        if ($LASTEXITCODE -ne 0) { throw "git fetch failed for wenet-e2e/wenet @ $PinnedWenetCommit" }
        & git -C $UpstreamWenetDir checkout --detach FETCH_HEAD
        if ($LASTEXITCODE -ne 0) { throw "git checkout failed for wenet-e2e/wenet @ $PinnedWenetCommit" }
    }
}
if (-not (Test-Path (Join-Path $UpstreamWenetDir "runtime\core\grpc\grpc_server.cc"))) {
    throw "Could not locate WeNet upstream worktree at: $UpstreamWenetDir"
}

$ProtoCoreDir = Join-Path $UpstreamWenetDir "runtime\core"
$ProtoFile = Join-Path $ProtoCoreDir "grpc\wenet.proto"
$PluginExe = (Get-Command grpc_cpp_plugin.exe).Source

$UpstreamBaseCommit = (& git -C $UpstreamWenetDir rev-parse --short $PinnedWenetCommit 2>$null)
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($UpstreamBaseCommit)) {
    $UpstreamBaseCommit = (& git -C $UpstreamWenetDir rev-parse --short HEAD)
}

# Prepare unpatched grpc_server.cc from pinned base commit and apply wenet_server_fix.patch to patched copy
$UnpatchedServerCc = Join-Path $UnpatchedDir "grpc_server.cc"
$BaseServerContent = & git -C $UpstreamWenetDir show "${UpstreamBaseCommit}:runtime/core/grpc/grpc_server.cc"
if ($LASTEXITCODE -ne 0) {
    Copy-Item (Join-Path $ProtoCoreDir "grpc\grpc_server.cc") $UnpatchedServerCc -Force
} else {
    [System.IO.File]::WriteAllText($UnpatchedServerCc, ($BaseServerContent -join "`r`n") + "`r`n", [System.Text.UTF8Encoding]::new($false))
}

$PatchedServerCc = Join-Path $PatchedDir "grpc_server.cc"
$PatchFile = Join-Path $ReproDir "wenet_server_fix.patch"
$PatchRaw = (Get-Content -Raw $PatchFile) -replace 'a/runtime/core/grpc/grpc_server\.cc', 'a/grpc_server.cc' -replace 'b/runtime/core/grpc/grpc_server\.cc', 'b/grpc_server.cc'
$StageDir = Join-Path $env:TEMP ("wenet_patch_stage_" + [System.Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Force -Path $StageDir | Out-Null
try {
    $StageServerCc = Join-Path $StageDir "grpc_server.cc"
    $TmpPatch = Join-Path $StageDir "local_server_fix.patch"
    Copy-Item $UnpatchedServerCc $StageServerCc -Force
    [System.IO.File]::WriteAllText($TmpPatch, $PatchRaw, [System.Text.UTF8Encoding]::new($false))
    & git -C $StageDir apply --ignore-whitespace --unsafe-paths $TmpPatch
    if ($LASTEXITCODE -ne 0) { throw "git apply failed for $PatchFile" }
    $PatchedText = [System.IO.File]::ReadAllText($StageServerCc)
    if ($PatchedText -notmatch 'has_decode_config\(') {
        throw "Patched grpc_server.cc is missing expected has_decode_config() guard after applying $PatchFile"
    }
    Copy-Item $StageServerCc $PatchedServerCc -Force
} finally {
    Remove-Item $StageDir -Recurse -Force -ErrorAction SilentlyContinue
}
$UpstreamFixLabel = "wenet_server_fix.patch on $UpstreamBaseCommit"

# Compile wenet.proto via real protoc + grpc_cpp_plugin
& protoc.exe -I "$ProtoCoreDir" --cpp_out="$GenDir" --grpc_out="$GenDir" --plugin="protoc-gen-grpc=$PluginExe" "$ProtoFile"
if ($LASTEXITCODE -ne 0) { throw "protoc failed" }

$abslLibs = @(
    Get-ChildItem "C:\msys64\ucrt64\lib\libabsl*.a" |
    ForEach-Object { "-l" + ($_.Name -replace '^lib', '' -replace '(\.dll)?\.a$', '') } |
    Sort-Object -Unique
)
$linkFlags = @("-L`"C:\msys64\ucrt64\lib`"", "-Wl,--start-group", "-lgrpc++", "-lgrpc", "-lgpr", "-lprotobuf") + $abslLibs + @("-Wl,--end-group", "-lws2_32")

$incFlags = @(
    "-I`"$ReproDir\harness_stubs`"",
    "-I`"$GenDir`"",
    "-I`"$ProtoCoreDir`"",
    "-I`"C:\msys64\ucrt64\include`""
)

& g++ -std=c++17 -O2 -Wall -Wextra -c "$GenDir\grpc\wenet.pb.cc" -o "$BuildDir\wenet.pb.o" -I"$GenDir" -I"C:\msys64\ucrt64\include"
if ($LASTEXITCODE -ne 0) { throw "wenet.pb.cc compile failed" }
& g++ -std=c++17 -O2 -Wall -Wextra -c "$GenDir\grpc\wenet.grpc.pb.cc" -o "$BuildDir\wenet.grpc.pb.o" -I"$GenDir" -I"C:\msys64\ucrt64\include"
if ($LASTEXITCODE -ne 0) { throw "wenet.grpc.pb.cc compile failed" }

$commonObjs = @(
    (Join-Path $BuildDir "wenet.pb.o"),
    (Join-Path $BuildDir "wenet.grpc.pb.o")
)

# 1. Unpatched upstream server (base commit)
& g++ -std=c++17 -O2 -Wall -Wextra @incFlags "$UnpatchedServerCc" "$ReproDir\src\wenet_server_runner.cpp" @commonObjs -o "$BuildDir\wenet_server_unpatched.exe" @linkFlags
if ($LASTEXITCODE -ne 0) { throw "unpatched server compile failed" }

# 2. Patched upstream server (wenet_server_fix.patch applied to unpatched base)
& g++ -std=c++17 -O2 -Wall -Wextra @incFlags "$PatchedServerCc" "$ReproDir\src\wenet_server_runner.cpp" @commonObjs -o "$BuildDir\wenet_server_patched.exe" @linkFlags
if ($LASTEXITCODE -ne 0) { throw "patched server compile failed" }

# 3. Real unmodified GrpcClient + test driver
& g++ -std=c++17 -O2 -Wall -Wextra @incFlags "$ProtoCoreDir\grpc\grpc_client.cc" "$ReproDir\src\wenet_test_client.cpp" @commonObjs -o "$BuildDir\wenet_test_client.exe" @linkFlags
if ($LASTEXITCODE -ne 0) { throw "test client compile failed" }

$OutputFile = Join-Path $ReproDir "TEST_OUTPUT.txt"
$Log = New-Object System.Collections.Generic.List[string]

$Log.Add("========================================================================")
$Log.Add("WeNet ASR Real libgrpc++ 1.72.0 Runtime Verification Output")
$Log.Add("Timestamp (UTC): " + [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ"))
$Log.Add("Upstream Repo: $UpstreamWenetDir")
$Log.Add("Unpatched Base Commit: $UpstreamBaseCommit")
$Log.Add("Patched Variant: $UpstreamFixLabel")
$Log.Add("protoc: " + (& protoc.exe --version))
$Log.Add("g++: " + ((& g++ --version) | Select-Object -First 1))
$Log.Add("========================================================================")
$Log.Add("")
$Log.Add(">>> PART 1: UNPATCHED UPSTREAM BASELINE ($UpstreamBaseCommit) <<<")
$Log.Add("")

$ErrorActionPreference = "Continue"
$unpatchedOut = & "$BuildDir\wenet_test_client.exe" --server-exe "$BuildDir\wenet_server_unpatched.exe" --mode unpatched 2>&1 | ForEach-Object { $_.ToString() }
$unpatchedCode = $LASTEXITCODE
$ErrorActionPreference = "Stop"
foreach ($line in $unpatchedOut) { $Log.Add($line) }
$Log.Add("UNPATCHED_SUITE_EXIT_CODE=$unpatchedCode")
$Log.Add("")
$Log.Add("========================================================================")
$Log.Add(">>> PART 2: PATCHED FIX VERIFICATION ($UpstreamFixLabel, Test 3D) <<<")
$Log.Add("")

$ErrorActionPreference = "Continue"
$patchedOut = & "$BuildDir\wenet_test_client.exe" --server-exe "$BuildDir\wenet_server_patched.exe" --mode patched 2>&1 | ForEach-Object { $_.ToString() }
$patchedCode = $LASTEXITCODE
$ErrorActionPreference = "Stop"
foreach ($line in $patchedOut) { $Log.Add($line) }
$Log.Add("PATCHED_SUITE_EXIT_CODE=$patchedCode")

[System.IO.File]::WriteAllLines($OutputFile, $Log, [System.Text.UTF8Encoding]::new($false))
Write-Host "Wrote $OutputFile (UNPATCHED=$unpatchedCode, PATCHED=$patchedCode)"

if ($unpatchedCode -ne 0 -or $patchedCode -ne 0) {
    exit 1
}
exit 0
