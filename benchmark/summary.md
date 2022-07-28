### Benchmark summary: 
|Scenarios that create local files | File Size | File Count |
|:---|:---|:---|
| upload small files | 1M | 10 |
| upload large files | 1M | 1 |

|Scenario| Summary |
|:---|:---|
| upload small files | 'version:v2.0.0' ran 1.08 ± 0.06 times faster than 'version:v1.4.0' |
| download small files | 'version:v2.0.0' ran 1.01 ± 0.02 times faster than 'version:v1.4.0' |
| upload large files | 'version:v1.4.0' ran 1.14 ± 0.17 times faster than 'version:v2.0.0' |
| download large files | 'version:v2.0.0' ran 1.05 ± 0.10 times faster than 'version:v1.4.0' |
| remove small files | 'version:v2.0.0' ran 1.02 times faster than 'version:v1.4.0' |
| remove large files | 'version:v2.0.0' ran 1.25 times faster than 'version:v1.4.0' |

 ### Detailed summary: 
 |Scenario| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
 |:---|:---|---:|---:|---:|---:|
 | upload small files | `version:v2.0.0` | 187.3 ± 9.5 | 178.4 | 201.3 | 1.00 |
 | upload small files | `version:v1.4.0` | 201.5 ± 5.3 | 192.7 | 205.4 | 1.08 ± 0.06 |
 | download small files | `version:v2.0.0` | 19.6 ± 0.1 | 19.4 | 19.7 | 1.00 |
 | download small files | `version:v1.4.0` | 19.7 ± 0.4 | 19.2 | 20.2 | 1.01 ± 0.02 |
 | upload large files | `version:v2.0.0` | 45.7 ± 6.6 | 38.1 | 55.8 | 1.14 ± 0.17 |
 | upload large files | `version:v1.4.0` | 40.1 ± 1.1 | 39.3 | 41.9 | 1.00 |
 | download large files | `version:v2.0.0` | 14.0 ± 0.5 | 13.3 | 14.6 | 1.00 |
 | download large files | `version:v1.4.0` | 14.6 ± 1.2 | 13.3 | 16.4 | 1.05 ± 0.10 |
 | remove small files | `version:v2.0.0` | 34.4 | 34.4 | 34.4 | 1.00 |
 | remove small files | `version:v1.4.0` | 35.0 | 35.0 | 35.0 | 1.02 |
 | remove large files | `version:v2.0.0` | 26.0 | 26.0 | 26.0 | 1.00 |
 | remove large files | `version:v1.4.0` | 32.6 | 32.6 | 32.6 | 1.25 |
