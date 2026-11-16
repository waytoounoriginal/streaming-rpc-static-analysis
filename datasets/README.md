# Evaluation Datasets (`datasets/`)

All static and runtime verification inputs are organized into two disjoint datasets with zero overlap between controlled case-study examples and unmodified open-source repository code:

1. **[`01_initial_case_study/`](./01_initial_case_study/README.md) (`N = 10` handlers, `2` self-contained `.proto` services):**
   - Controlled end-to-end client + dual-server gRPC streaming implementations (`01_pingpong_handshake` for Level 1 ordering/termination & endpoint duality; `02_config_stream` for Level 2 `oneof` payload progression & post-`WritesDone()` `Write()`).
   - Verified both statically via [`verify_static.py`](./01_initial_case_study/verify_static.py) (`10/10`) and dynamically on live `libgrpc++ 1.72.0` HTTP/2 loopback via [`build_and_run.ps1`](./01_initial_case_study/build_and_run.ps1).

2. **`02_unmodified_oss/` (`N = 16` upstream methods across `6` pinned OSS repositories):**
   - Declarative specification in [`manifest.json`](./02_unmodified_oss/manifest.json), external binary session contracts in [`contracts/`](./02_unmodified_oss/contracts/), and runner [`verify_oss.py`](./02_unmodified_oss/verify_oss.py).
   - Evaluates unmodified upstream C++ AST dumps across `grpc/grpc` (`30d65f9`), `apache/arrow` (`fb767a7`), `BuildGrid/buildbox-common` (`0926670`), `wenet-e2e/wenet` (`d170596`), `triton-inference-server/client` (`13095d2`), and `googleapis/google-cloud-cpp` (`c0ab3df`) with **no repo-specific analyzer heuristics**.
   - AST JSON dumps (`56.7 MB`, `15` files) reside locally under gitignored `.cache/` (or `$REPOS_AST_DIR`).
