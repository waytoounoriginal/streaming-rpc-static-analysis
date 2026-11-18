# Quantitative Evaluation & Benchmark Metrics (`evaluation/collect_metrics.py`)

- **Environment:** Windows-11-10.0.22631-SP0 | Python `3.12.6` | `clang version 19.1.5`
- **Timing Protocol:** Post-warmup median and inclusive IQR (Compiler: 3+15; DS1: 1+5; DS2: 1+5).
- $O_{\text{build}} = (T_{\text{pipeline}} - T_{\text{plain}}) / T_{\text{plain}}$ (paired end-to-end overhead over plain `clang++ -fsyntax-only`); $R_2 = T_{\text{check}} / (T_{\text{decode}} + T_{\text{check}})$ (cached AST JSON).

## 1. Controlled Case Study vs. Unmodified OSS Ground-Truth Controls & Category D Triage

### 1a. Controlled Dataset 1 (`datasets/01_initial_case_study/`)

| Dataset | Cases ($N$) | TP / FP / TN / FN | Prec | Rec | Spec | $F_1$ | Plain Med [IQR] (ms) | Pipeline Med [IQR] (ms) | Check Med [IQR] (ms) | $O_{\text{build}}$ Med [IQR] (%) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Dataset 1: Initial Case Study | 10 | 6 / 0 / 4 / 0 | 1.000 | 1.000 | 1.000 | 1.000 | 3965.5 [152.1] | 4182.8 [182.1] | 2.95 [0.57] | 3.95% [0.92%] |

### 1b. Unmodified OSS Baseline (`datasets/02_unmodified_oss/`, Provenance-Separated)

| Cohort (Adjudication) | Cases ($N$) | Outcome Split | Specificity | Decode Med [IQR] (ms) | Check Med [IQR] (ms) | $R_2$ Med [IQR] (%) | Max Loop Configs |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Confirmed Conforming Controls | 7 | TN=2 / FP=5 | 0.286 | 4.8 [12.3] | 1.62 [0.63] | 24.3% [7.6%] | 17 |
| Category D Repro Candidates [Unconfirmed] | 9 | Flagged=9 / Missed=0 | -- (unconfirmed) | 4.8 [19.8] | 1.61 [0.97] | 22.3% [17.0%] | 8 |

> **Dataset 2 Provenance Note:** Evaluated on cached upstream Clang AST JSONs (41 diagnostics total without repo-specific heuristics). Dataset 1 max loop configs = 2, Dataset 2 Controls = 17, Dataset 2 Candidates = 8. Category D candidates remain unconfirmed upstream and excluded from confirmed Recall.

## 2. Session Compiler Overhead (`.proto` `MethodOptions` $\rightarrow$ Dual `grpc.session.v1` FSMs)

| Protocol Schema | RPCs | Opt Lines | Compile Med [IQR] (ms) | Client $\|Q\| / \|\delta\|$ | Dual Server $\|Q\| / \|\delta\|$ |
| :--- | :---: | :---: | :---: | :---: | :---: |
| `handshake.proto` | 1 | 1 | 0.35 [0.02] | 6 / 6 | 6 / 6 |
| `config_stream.proto` | 1 | 1 | 0.38 [0.01] | 5 / 5 | 5 / 5 |
| `wenet.proto` | 1 | 1 | 0.57 [0.02] | 5 / 6 | 4 / 5 |
| `real_repos.proto` | 6 | 7 | 1.19 [0.38] | 28 / 31 | - / - |

## 3. Dataset 1 Breakdown (`datasets/01_initial_case_study/`, 10 Handlers)

| Case | SLOC | Annots | Plain Med [IQR] (ms) | Pipeline Med [IQR] (ms) | Check Med (ms) | $O_{\text{build}}$ (%) | Verdict | Diags |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Ex1 Client Good | 63 | 0 | 3903.4 [182.9] | 4030.4 [21.8] | 3.14 | 3.54% [0.56%] | PASS (OK) | 0 |
| Ex1 Client Bad Swap | 27 | 0 | 4106.8 [113.8] | 4264.1 [91.8] | 2.38 | 2.22% [2.15%] | FAIL (OK) | 4 |
| Ex1 Client Bad Leak | 36 | 0 | 3998.6 [115.7] | 4062.1 [24.2] | 2.58 | 3.53% [2.97%] | FAIL (OK) | 3 |
| Ex1 Server Dual Good | 32 | 0 | 3868.1 [23.5] | 4030.1 [25.4] | 2.50 | 4.53% [0.70%] | PASS (OK) | 0 |
| Ex1 Server Bad Order | 17 | 0 | 4088.4 [74.0] | 4417.1 [106.4] | 3.09 | 4.21% [4.83%] | FAIL (OK) | 4 |
| Ex1 Server Missing Summ. | 24 | 0 | 4072.0 [22.2] | 4260.2 [49.4] | 3.54 | 3.81% [1.13%] | FAIL (OK) | 1 |
| Ex2 Client Good (oneof) | 56 | 0 | 4031.8 [20.0] | 4226.8 [38.7] | 3.71 | 5.10% [1.56%] | PASS (OK) | 0 |
| Ex2 Client Bad oneof | 28 | 0 | 3929.4 [125.1] | 4164.1 [232.3] | 3.04 | 3.00% [2.98%] | FAIL (OK) | 5 |
| Ex2 Client Bad PostClose | 34 | 0 | 3899.9 [45.9] | 4201.6 [89.9] | 2.55 | 6.75% [3.32%] | FAIL (OK) | 1 |
| Ex2 Server Dual Good | 35 | 0 | 3932.5 [33.4] | 4092.8 [11.1] | 2.86 | 4.08% [2.60%] | PASS (OK) | 0 |

## 4. Dataset 2 Breakdown (`datasets/02_unmodified_oss/`, 16 Targets)

| ID | Case | Adjudication | Spec | LOC | AST MB | Decode Med (ms) | Check Med (ms) | $R_2$ (%) | Verdict | Diags |
| :--- | :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `OSS-01` | grpc/grpc Intercept (CD-01) | Cat-D Unconf. | `custom` | 78 | 1.91 | 3.96 | 1.86 | 33.4% | FLAG-REPRO | 2 |
| `OSS-02` | arrow GrpcResultStream (CTL-01) | Conforming | `custom` | 29 | 2.80 | 5.46 | 1.82 | 24.6% | TN (PASS) | 0 |
| `OSS-03` | arrow ListFlights (CD-02/03) | Cat-D Unconf. | `custom` | 23 | 1.51 | 3.28 | 1.61 | 33.0% | FLAG-REPRO | 2 |
| `OSS-04` | arrow Authenticate (CD-05/06) | Cat-D Unconf. | `custom` | 25 | 1.02 | 1.91 | 0.96 | 33.2% | FLAG-REPRO | 7 |
| `OSS-05` | buildbox StageTree (CD-07) | Cat-D Unconf. | `custom` | 66 | 2.72 | 6.11 | 1.48 | 20.1% | FLAG-REPRO | 6 |
| `OSS-06` | wenet GrpcClient (CTL-04) | Conforming | `custom` | 26 | 1.07 | 2.82 | 1.01 | 21.8% | FP (FLAG) | 2 |
| `OSS-07` | wenet Server (CD-12) | Cat-D Unconf. | `custom` | 21 | 2.02 | 4.83 | 1.46 | 22.3% | FLAG-REPRO | 3 |
| `OSS-08` | triton Good (CTL-03) | Conforming | `custom` | 161 | 20.93 | 61.71 | 19.76 | 24.3% | FP (FLAG) | 1 |
| `OSS-09` | triton Post-Stop (CD-11) | Cat-D Unconf. | `custom` | 161 | 20.93 | 50.55 | 10.12 | 16.3% | FLAG-REPRO | 4 |
| `OSS-10` | gcloud InsertObjectMedia | Conforming | `custom` | 81 | 2.24 | 4.75 | 1.62 | 25.1% | FP (FLAG) | 3 |
| `OSS-11` | gcloud UploadChunk | Conforming | `custom` | 69 | 1.93 | 3.91 | 1.36 | 25.8% | FP (FLAG) | 3 |
| `OSS-12` | arrow ListActions (CD-04/FP-01) | Cat-D Unconf. | `default-L1` | 19 | 0.57 | 2.64 | 1.38 | 34.3% | FLAG-REPRO | 2 |
| `OSS-13` | arrow DoFinish (CTL-01) | Conforming | `default-L1` | 36 | 1.25 | 4.28 | 0.64 | 12.7% | FP (FLAG) | 2 |
| `OSS-14` | buildbox upload(str) (CTL-02) | Conforming | `default-L1` | 67 | 9.25 | 27.25 | 1.82 | 6.3% | TN (PASS) | 0 |
| `OSS-15` | buildbox uploadFdRegion (CD-08) | Cat-D Unconf. | `default-L1` | 79 | 9.25 | 27.69 | 2.43 | 8.3% | FLAG-REPRO | 2 |
| `OSS-16` | buildbox download (CD-09) | Cat-D Unconf. | `default-L1` | 74 | 7.46 | 23.05 | 3.42 | 12.9% | FLAG-REPRO | 2 |
