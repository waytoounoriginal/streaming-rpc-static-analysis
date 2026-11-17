# Quantitative Evaluation & Benchmark Metrics (`evaluation/collect_metrics.py`)

- **Environment:** Windows-11-10.0.22631-SP0 | Python `3.12.6` | `clang version 19.1.5`
- **Timing Protocol:** Post-warmup median and inclusive IQR (Compiler: 3+15; DS1: 1+5; DS2: 1+5).
- $R_1 = T_{\text{check}} / (T_{\text{frontend}} + T_{\text{check}})$ (live Clang AST); $R_2 = T_{\text{check}} / (T_{\text{decode}} + T_{\text{check}})$ (cached AST JSON).

## 1. Controlled Case Study vs. Unmodified OSS Ground-Truth Controls & Category D Triage

### 1a. Controlled Dataset 1 (`datasets/01_initial_case_study/`)

| Dataset | Cases ($N$) | TP / FP / TN / FN | Prec | Rec | Spec | $F_1$ | Clang Med [IQR] (ms) | Check Med [IQR] (ms) | $R_1$ Med [IQR] (%) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Dataset 1: Initial Case Study | 10 | 6 / 0 / 4 / 0 | 1.000 | 1.000 | 1.000 | 1.000 | 4237.0 [395.5] | 3.38 [0.40] | 0.078% [0.007%] |

### 1b. Unmodified OSS Baseline (`datasets/02_unmodified_oss/`, Provenance-Separated)

| Cohort (Adjudication) | Cases ($N$) | Outcome Split | Specificity | Decode Med [IQR] (ms) | Check Med [IQR] (ms) | $R_2$ Med [IQR] (%) | Max Loop Configs |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Confirmed Conforming Controls | 7 | TN=2 / FP=5 | 0.286 | 5.7 [13.7] | 1.65 [0.38] | 23.9% [9.6%] | 17 |
| Category D Repro Candidates [Unconfirmed] | 9 | Flagged=9 / Missed=0 | -- (unconfirmed) | 5.7 [18.7] | 2.19 [1.06] | 20.0% [16.1%] | 8 |

> **Dataset 2 Provenance Note:** Evaluated on cached upstream Clang AST JSONs (41 diagnostics total without repo-specific heuristics). Dataset 1 max loop configs = 2, Dataset 2 Controls = 17, Dataset 2 Candidates = 8. Category D candidates remain unconfirmed upstream and excluded from confirmed Recall.

## 2. Session Compiler Overhead (`.proto` `MethodOptions` $\rightarrow$ Dual `grpc.session.v1` FSMs)

| Protocol Schema | RPCs | Opt Lines | Compile Med [IQR] (ms) | Client $\|Q\| / \|\delta\|$ | Dual Server $\|Q\| / \|\delta\|$ |
| :--- | :---: | :---: | :---: | :---: | :---: |
| `handshake.proto` | 1 | 1 | 0.36 [0.02] | 6 / 6 | 6 / 6 |
| `config_stream.proto` | 1 | 1 | 0.39 [0.01] | 5 / 5 | 5 / 5 |
| `wenet.proto` | 1 | 1 | 0.52 [0.01] | 5 / 6 | 4 / 5 |
| `real_repos.proto` | 6 | 7 | 0.92 [0.01] | 28 / 31 | - / - |

## 3. Dataset 1 Breakdown (`datasets/01_initial_case_study/`, 10 Handlers)

| Case | SLOC | Annots | Clang Med [IQR] (ms) | Check Med [IQR] (ms) | $R_1$ (%) | Verdict | Diags |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Ex1 Client Good | 63 | 0 | 4461.1 [392.8] | 3.50 [1.60] | 0.081% | PASS (OK) | 0 |
| Ex1 Client Bad Swap | 27 | 0 | 4337.7 [180.5] | 3.18 [0.84] | 0.073% | FAIL (OK) | 4 |
| Ex1 Client Bad Leak | 36 | 0 | 4510.6 [86.5] | 3.51 [0.45] | 0.079% | FAIL (OK) | 3 |
| Ex1 Server Dual Good | 32 | 0 | 4438.1 [106.0] | 3.55 [1.13] | 0.077% | PASS (OK) | 0 |
| Ex1 Server Bad Order | 17 | 0 | 4653.2 [354.7] | 3.37 [0.03] | 0.073% | FAIL (OK) | 4 |
| Ex1 Server Missing Summ. | 24 | 0 | 4070.7 [318.2] | 2.53 [0.25] | 0.058% | FAIL (OK) | 1 |
| Ex2 Client Good (oneof) | 56 | 0 | 4136.2 [136.7] | 3.77 [0.94] | 0.094% | PASS (OK) | 0 |
| Ex2 Client Bad oneof | 28 | 0 | 3983.2 [70.1] | 3.38 [1.20] | 0.086% | FAIL (OK) | 5 |
| Ex2 Client Bad PostClose | 34 | 0 | 3995.2 [3.8] | 3.09 [0.47] | 0.078% | FAIL (OK) | 1 |
| Ex2 Server Dual Good | 35 | 0 | 4056.2 [88.7] | 2.61 [0.37] | 0.066% | PASS (OK) | 0 |

## 4. Dataset 2 Breakdown (`datasets/02_unmodified_oss/`, 16 Targets)

| ID | Case | Adjudication | Spec | LOC | AST MB | Decode Med (ms) | Check Med (ms) | $R_2$ (%) | Verdict | Diags |
| :--- | :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `OSS-01` | grpc/grpc Intercept (CD-01) | Cat-D Unconf. | `custom` | 78 | 1.91 | 4.25 | 1.88 | 33.6% | FLAG-REPRO | 2 |
| `OSS-02` | arrow GrpcResultStream (CTL-01) | Conforming | `custom` | 29 | 2.80 | 5.74 | 1.80 | 23.9% | TN (PASS) | 0 |
| `OSS-03` | arrow ListFlights (CD-02/03) | Cat-D Unconf. | `custom` | 23 | 1.51 | 5.17 | 2.58 | 32.2% | FLAG-REPRO | 2 |
| `OSS-04` | arrow Authenticate (CD-05/06) | Cat-D Unconf. | `custom` | 25 | 1.02 | 2.18 | 0.99 | 31.3% | FLAG-REPRO | 7 |
| `OSS-05` | buildbox StageTree (CD-07) | Cat-D Unconf. | `custom` | 66 | 2.72 | 9.12 | 2.19 | 19.4% | FLAG-REPRO | 6 |
| `OSS-06` | wenet GrpcClient (CTL-04) | Conforming | `custom` | 26 | 1.07 | 4.09 | 0.98 | 19.0% | FP (FLAG) | 2 |
| `OSS-07` | wenet Server (CD-12) | Cat-D Unconf. | `custom` | 21 | 2.02 | 5.69 | 1.52 | 20.0% | FLAG-REPRO | 3 |
| `OSS-08` | triton Good (CTL-03) | Conforming | `custom` | 161 | 20.93 | 55.80 | 19.00 | 25.4% | FP (FLAG) | 1 |
| `OSS-09` | triton Post-Stop (CD-11) | Cat-D Unconf. | `custom` | 161 | 20.93 | 51.97 | 9.82 | 16.1% | FLAG-REPRO | 4 |
| `OSS-10` | gcloud InsertObjectMedia | Conforming | `custom` | 81 | 2.24 | 4.51 | 1.65 | 26.9% | FP (FLAG) | 3 |
| `OSS-11` | gcloud UploadChunk | Conforming | `custom` | 69 | 1.93 | 3.79 | 1.35 | 26.3% | FP (FLAG) | 3 |
| `OSS-12` | arrow ListActions (CD-04/FP-01) | Cat-D Unconf. | `default-L1` | 19 | 0.57 | 2.33 | 1.30 | 35.8% | FLAG-REPRO | 2 |
| `OSS-13` | arrow DoFinish (CTL-01) | Conforming | `default-L1` | 36 | 1.25 | 9.49 | 1.48 | 13.5% | FP (FLAG) | 2 |
| `OSS-14` | buildbox upload(str) (CTL-02) | Conforming | `default-L1` | 67 | 9.25 | 26.56 | 1.79 | 6.4% | TN (PASS) | 0 |
| `OSS-15` | buildbox uploadFdRegion (CD-08) | Cat-D Unconf. | `default-L1` | 79 | 9.25 | 27.54 | 2.31 | 7.4% | FLAG-REPRO | 2 |
| `OSS-16` | buildbox download (CD-09) | Cat-D Unconf. | `default-L1` | 74 | 7.46 | 22.97 | 3.19 | 12.3% | FLAG-REPRO | 2 |
