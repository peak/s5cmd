### Benchmark summary: 
|Scenarios that create local files | File Size | File Count |
|:---|:---|:---|
| upload small files | 1M | 1 |
| upload large files | 10M | 1 |

|Scenario| Summary |
|:---|:---|
| upload small files | 'version:v2.0.0' ran 9.60 times faster than 'version:v1.4.0' |
| download small files | 'version:v2.0.0' ran 1.01 times faster than 'version:v1.4.0' |
| upload large files | 'version:v1.4.0' ran 1.03 times faster than 'version:v2.0.0' |
| download large files | 'version:v2.0.0' ran 1.01 times faster than 'version:v1.4.0' |
| remove small files | 'version:v1.4.0' ran 1.00 times faster than 'version:v2.0.0' |
| remove large files | 'version:v2.0.0' ran 1.46 times faster than 'version:v1.4.0' |

 ### Detailed summary: 
 |Scenario| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
 |:---|:---|---:|---:|---:|---:|
 | upload small files | `version:v2.0.0` | 48.8 | 48.8 | 48.8 | 1.00 |
 | upload small files | `version:v1.4.0` | 469.0 | 469.0 | 469.0 | 9.60 |
 | download small files | `version:v2.0.0` | 12.5 | 12.5 | 12.5 | 1.00 |
 | download small files | `version:v1.4.0` | 12.6 | 12.6 | 12.6 | 1.01 |
 | upload large files | `version:v2.0.0` | 136.1 | 136.1 | 136.1 | 1.03 |
 | upload large files | `version:v1.4.0` | 131.8 | 131.8 | 131.8 | 1.00 |
 | download large files | `version:v2.0.0` | 19.5 | 19.5 | 19.5 | 1.00 |
 | download large files | `version:v1.4.0` | 19.7 | 19.7 | 19.7 | 1.01 |
 | remove small files | `version:v2.0.0` | 46.1 | 46.1 | 46.1 | 1.00 |
 | remove small files | `version:v1.4.0` | 46.0 | 46.0 | 46.0 | 1.00 |
 | remove large files | `version:v2.0.0` | 22.0 | 22.0 | 22.0 | 1.00 |
 | remove large files | `version:v1.4.0` | 32.2 | 32.2 | 32.2 | 1.46 |
