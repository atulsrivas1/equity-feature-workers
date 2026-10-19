# EQ140 frozen supplied-worker results

Measured source: `be4d6667e9b46981cf430c86888b52c7812b0424`. Author formatting follows completed separate evidence audit.

Six sequential baselines and48 alternatives:2 warmups and7 retained samples per cohort (108 warmups/378 retained;486 owned Jobs). All baselines froze before candidates. No default mode change.

## Sequential baselines

| Series/profile | Median wall (s) | Median Job CPU (s) | Max sampled RSS (MiB) | Frozen improvement floor |
| --- | ---: | ---: | ---: | ---: |
| compute-balanced | 12.292977 | 12.281250 | 62.60 | 12.11% |
| compute-skewed | 3.689577 | 3.671875 | 55.91 | 17.25% |
| compute-small | 0.206544 | 0.203125 | 49.29 | 8.11% |
| e2e-balanced | 44.815368 | 44.421875 | 70.42 | 7.64% |
| e2e-skewed | 14.780344 | 14.640625 | 59.99 | 12.33% |
| e2e-small | 1.906184 | 1.796875 | 49.99 | 5.00% |

## Every alternative

Gate failures refer to the frozen rule; a wall-time improvement alone does not establish adoption. Positive improvement means less elapsed time.

| Series/profile/mode/degree | Median wall (s) | Wall improvement | Qualifies | Failed gates |
| --- | ---: | ---: | --- | --- |
| compute-balanced-process-1 | 14.246930 | -15.89% | no | memory, small_p95, wall |
| compute-balanced-process-2 | 12.602635 | -2.52% | no | memory, small_p95, wall |
| compute-balanced-process-4 | 11.529607 | 6.21% | no | cpu, memory, small_p95, wall |
| compute-balanced-process-8 | 11.362767 | 7.57% | no | cpu, memory, small_p95, wall |
| compute-balanced-thread-1 | 12.183516 | 0.89% | no | wall |
| compute-balanced-thread-2 | 12.543222 | -2.04% | no | wall |
| compute-balanced-thread-4 | 12.572402 | -2.27% | no | wall |
| compute-balanced-thread-8 | 12.838600 | -4.44% | no | memory, wall |
| compute-skewed-process-1 | 4.869058 | -31.97% | no | cpu, memory, small_p95, wall |
| compute-skewed-process-2 | 4.372135 | -18.50% | no | cpu, memory, small_p95, wall |
| compute-skewed-process-4 | 4.207381 | -14.03% | no | cpu, memory, small_p95, wall |
| compute-skewed-process-8 | 4.176714 | -13.20% | no | cpu, memory, small_p95, wall |
| compute-skewed-thread-1 | 3.694639 | -0.14% | no | wall |
| compute-skewed-thread-2 | 3.818644 | -3.50% | no | wall |
| compute-skewed-thread-4 | 3.820217 | -3.54% | no | wall |
| compute-skewed-thread-8 | 3.856715 | -4.53% | no | wall |
| compute-small-process-1 | 1.051048 | -408.87% | no | cpu, memory, small_p95, wall |
| compute-small-process-2 | 1.017974 | -392.86% | no | cpu, memory, small_p95, wall |
| compute-small-process-4 | 1.072777 | -419.39% | no | cpu, memory, small_p95, wall |
| compute-small-process-8 | 1.024353 | -395.95% | no | cpu, memory, small_p95, wall |
| compute-small-thread-1 | 0.207193 | -0.31% | no | wall |
| compute-small-thread-2 | 0.208142 | -0.77% | no | wall |
| compute-small-thread-4 | 0.211555 | -2.43% | no | wall |
| compute-small-thread-8 | 0.210931 | -2.12% | no | wall |
| e2e-balanced-process-1 | 49.362971 | -10.15% | no | memory, small_p95, wall |
| e2e-balanced-process-2 | 47.608468 | -6.23% | no | memory, small_p95, wall |
| e2e-balanced-process-4 | 46.170581 | -3.02% | no | memory, small_p95, wall |
| e2e-balanced-process-8 | 45.770914 | -2.13% | no | memory, small_p95, wall |
| e2e-balanced-thread-1 | 45.490735 | -1.51% | no | wall |
| e2e-balanced-thread-2 | 45.218028 | -0.90% | no | wall |
| e2e-balanced-thread-4 | 45.853697 | -2.32% | no | wall |
| e2e-balanced-thread-8 | 46.143517 | -2.96% | no | wall |
| e2e-skewed-process-1 | 15.603049 | -5.57% | no | memory, small_p95, wall |
| e2e-skewed-process-2 | 15.497896 | -4.85% | no | memory, small_p95, wall |
| e2e-skewed-process-4 | 15.144922 | -2.47% | no | memory, small_p95, wall |
| e2e-skewed-process-8 | 15.016936 | -1.60% | no | cpu, memory, small_p95, wall |
| e2e-skewed-thread-1 | 14.644114 | 0.92% | no | wall |
| e2e-skewed-thread-2 | 14.417015 | 2.46% | no | wall |
| e2e-skewed-thread-4 | 14.629880 | 1.02% | no | wall |
| e2e-skewed-thread-8 | 14.421672 | 2.43% | no | wall |
| e2e-small-process-1 | 2.702635 | -41.78% | no | cpu, memory, small_p95, wall |
| e2e-small-process-2 | 2.733341 | -43.39% | no | cpu, memory, small_p95, wall |
| e2e-small-process-4 | 2.676690 | -40.42% | no | cpu, memory, small_p95, wall |
| e2e-small-process-8 | 2.677565 | -40.47% | no | cpu, memory, small_p95, wall |
| e2e-small-thread-1 | 1.886151 | 1.05% | no | wall |
| e2e-small-thread-2 | 1.865755 | 2.12% | no | wall |
| e2e-small-thread-4 | 1.866202 | 2.10% | no | wall |
| e2e-small-thread-8 | 1.877273 | 1.52% | no | wall |

0 qualifying alternatives; 48 nonwins retain sequential fallback. These are workload-specific synthetic observations, not a universal ranking.

Full precision, resource distributions and retained stage/byte observations remain in the [summary](EQ140_STUDY_SUMMARY.json). Exact raw JSON bytes, warmups and native sampling records remain in the [compressed archive](EQ140_BE4_FROZEN_STUDY.tar.gz), with its [closed manifest and SHA256](EQ140_STUDY_ARCHIVE.json). See the [separate evidence audit](EQ140_BE4_MEASUREMENT_REVIEW.md), [measured-source applicability](EQ140_BE4_FF_MEASUREMENT_APPLICABILITY_REVIEW.md) and [ff artifact component review](EQ140_FF_HOSTED_ARTIFACT_PUBLIC_REVIEW.md) for scope and limitations.

Compute is the admitted phase including execution/SDK validation/pool shutdown, excluding runtime admission/postchecks. Cold E2E includes fresh creation/import/source/admission/pool/transport/publication/lookup/readback/runtime checks/reporting. Per-task stage probes can overlap and must not be summed into makespan.

CPU includes the owned process tree and terminated children. RSS is sampled concurrent working set and can miss transient peaks; it is neither physical enforcement nor peak private commit. External observer CPU is recorded separately. Byte observations do not count every physical copy.

No persistent-pool, provider/live/SLA, physical durability, Linux speed or fastest-library claim. Separate audit limitations include retained SDK hashes rather than nonexistent raw SDK bodies and no independently signed execution chronology.
