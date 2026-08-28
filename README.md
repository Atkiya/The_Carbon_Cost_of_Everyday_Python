# The Carbon Cost of Everyday Python
## Reproducible Results and Statistical Analysis

This repository contains the analysis notebook and result summary for the study:

**The Carbon Cost of Everyday Python: A Reproducible Study of Energy-Efficient Data Processing Practices**

The study evaluates how **processing implementation, file format, numeric data type, categorical representation, and execution mode** influence the energy consumption of a fixed Python data-processing workload.

The experiment contains:

- **128 unique configurations**
- **10 measured repetitions per configuration**
- **1,280 measured benchmark runs**
- **8 processing implementations**
- **4 binary configuration factors**
- energy as the primary outcome
- runtime and peak memory as secondary outcomes

The complete statistical workflow is available in:

```text
Data_Analysis.ipynb
```

---

# 1. Main Result

The most energy-efficient tested configuration was:

```text
Polars Eager
+ Parquet
+ Float32
+ Object
+ Parallel
```

Its measured configuration-level results were:

| Metric | Result |
|---|---:|
| Mean energy | **8.98 J** |
| 95% CI for mean energy | **8.67–9.30 J** |
| Mean runtime | **0.79 s** |
| Mean peak memory | **806.45 MB** |
| Number of measured repetitions | **10** |

The highest-energy configuration was:

```text
Dask PyArrow
+ CSV
+ Float64
+ Category
+ Parallel
```

| Metric | Result |
|---|---:|
| Mean energy | **637.88 J** |
| Mean runtime | **48.71 s** |
| Mean peak memory | **808.98 MB** |
| Number of measured repetitions | **10** |

Compared with the highest-energy configuration, the lowest-energy configuration:

- used approximately **71.03× less energy**;
- reduced mean energy by approximately **98.59%**;
- was approximately **61.56× faster**;
- reduced mean runtime by approximately **98.38%**.

This result shows that software configuration choices can produce very large differences even when the same logical data-processing workload is performed.

---

# 2. Overall Benchmark Results

Across all **1,280 measured runs**:

| Metric | Energy | Runtime | Peak Memory |
|---|---:|---:|---:|
| Mean | **124.86 J** | **9.23 s** | **1029.31 MB** |
| Median | **31.49 J** | **2.38 s** | **809.22 MB** |

The large difference between the energy mean and median reflects the presence of several high-energy configurations, especially the Dask configurations.

---

# 3. Mean Results by Processing Implementation

Each processing implementation represents:

```text
16 configurations × 10 repetitions = 160 measured runs
```

The mean results were:

| Processing implementation | Mean energy (J) | Mean runtime (s) | Mean peak memory (MB) |
|---|---:|---:|---:|
| **Polars Eager** | **14.58** | **1.26** | 800.06 |
| **Polars Lazy** | **21.85** | **1.71** | 891.22 |
| **DuckDB** | **22.72** | **1.71** | 671.56 |
| **Pandas NumPy** | **30.88** | **2.11** | 1228.52 |
| **Pandas PyArrow** | **31.80** | **2.18** | 1186.11 |
| **Python Loops** | **242.01** | **17.67** | 2005.43 |
| **Dask Default** | **309.15** | **23.02** | 738.97 |
| **Dask PyArrow** | **325.85** | **24.19** | 712.62 |

### Interpretation

Processing implementation was the strongest practical factor in the experiment.

Polars Eager had the lowest implementation-level mean energy consumption, followed by Polars Lazy and DuckDB.

Dask Default and Dask PyArrow had the highest implementation-level mean energy consumption for this fixed single-machine workload.

These values should be interpreted for the tested workload and hardware rather than as universal rankings for all possible Python workloads.

---

# 4. File Format Results

Across all implementations:

| File format | Mean energy (J) | Mean runtime (s) | Mean peak memory (MB) |
|---|---:|---:|---:|
| **CSV** | 135.11 | 10.13 | 1012.04 |
| **Parquet** | **114.60** | **8.34** | 1046.58 |

Overall, Parquet had lower mean energy and runtime than CSV.

However, this effect was implementation-dependent.

Parquet reduced mean energy for **7 of the 8 processing implementations**. Explicit Python Loops was the exception, where CSV had lower mean energy than Parquet.

Therefore, the result does **not** support the universal rule that Parquet will always use less energy.

---

# 5. Numeric Data Type Results

Across all implementations:

| Numeric type | Mean energy (J) | Mean runtime (s) | Mean peak memory (MB) |
|---|---:|---:|---:|
| Float32 | 126.04 | 9.30 | **1023.21** |
| Float64 | **123.67** | **9.16** | 1035.41 |

The overall difference between Float32 and Float64 was small compared with the differences caused by implementation, categorical representation, and file format.

The original-scale practical effect size for numeric data type was:

```text
partial η² = 0.005
```

This was a **very small practical effect**.

The results therefore do not support a simple rule that Float32 automatically reduces energy for this workload.

---

# 6. Categorical Representation Results

Across all implementations:

| Representation | Mean energy (J) | Mean runtime (s) | Mean peak memory (MB) |
|---|---:|---:|---:|
| **Object** | **68.32** | **4.93** | 1076.17 |
| Category | 181.39 | 13.53 | **982.45** |

At the overall level, category representation used less peak memory but considerably more energy and runtime.

However, the overall average hides a very strong interaction with processing implementation.

### Important implementation-specific pattern

Category representation:

- **reduced energy for Pandas NumPy**;
- **reduced energy for Pandas PyArrow**;
- **strongly increased energy for Dask Default**;
- **strongly increased energy for Dask PyArrow**;
- produced smaller or implementation-specific changes for the remaining implementations.

This interaction was one of the strongest findings in the experiment.

```text
Implementation × Category partial η² = 0.973
```

This means the energy impact of categorical representation cannot be understood independently of the implementation in which it is used.

---

# 7. Execution Mode Results

Across all implementations:

| Execution mode | Mean energy (J) | Mean runtime (s) | Mean peak memory (MB) |
|---|---:|---:|---:|
| **Single** | **123.10** | 9.34 | **814.17** |
| Parallel | 126.61 | **9.12** | 1244.46 |

At the overall level, parallel execution was slightly faster, but it did not reduce average energy and required considerably more peak memory.

Again, the effect depended on the processing implementation.

### Implementation-specific pattern

Parallel execution:

- reduced mean energy for **Polars Eager**;
- reduced mean energy for **Polars Lazy**;
- reduced mean energy for **DuckDB**;
- increased mean energy for **Pandas NumPy**;
- increased mean energy for **Pandas PyArrow**;
- increased mean energy for **Python Loops**.

Therefore:

> **Parallel execution should not be treated as an automatically energy-efficient choice.**

---

# 8. Mean Energy by Implementation and Factor Level

The following table summarizes the aggregated mean energy for each processing implementation and configuration-factor level.

| Implementation | Overall | CSV | Parquet | Object | Category | Float32 | Float64 | Single | Parallel |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Polars Eager | 14.58 | 18.04 | 11.13 | 14.08 | 15.09 | 14.41 | 14.75 | 16.48 | 12.68 |
| Polars Lazy | 21.85 | 27.76 | 15.94 | 21.80 | 21.91 | 21.68 | 22.03 | 24.80 | 18.91 |
| DuckDB | 22.72 | 25.36 | 20.08 | 22.22 | 23.23 | 22.89 | 22.56 | 24.16 | 21.29 |
| Pandas NumPy | 30.88 | 37.38 | 24.38 | 34.76 | 27.00 | 30.60 | 31.16 | 22.78 | 38.98 |
| Pandas PyArrow | 31.80 | 39.09 | 24.51 | 35.29 | 28.31 | 31.65 | 31.95 | 23.86 | 39.74 |
| Python Loops | 242.01 | 208.14 | 275.88 | 234.77 | 249.24 | 250.12 | 233.90 | 236.91 | 247.11 |
| Dask Default | 309.15 | 353.12 | 265.17 | 89.67 | 528.63 | 309.08 | 309.21 | 310.35 | 307.95 |
| Dask PyArrow | 325.85 | 371.98 | 279.72 | 93.98 | 557.72 | 327.90 | 323.79 | 325.47 | 326.23 |

All values are **mean processor-related energy in Joules**, calculated from the directly measured benchmark runs.

---

# 9. Statistical Model

The original-scale five-factor model was:

```text
Energy ~
Implementation
× FileFormat
× NumericType
× CategoricalRepresentation
× ExecutionMode
+ Block
```

The model explained approximately:

```text
R² = 0.992
```

or about **99.2% of the observed variation in energy**.

The original-scale model was retained for descriptive model quantities and partial eta-squared effect sizes.

---

# 10. Assumption Tests

The assumptions required for ordinary factorial ANOVA were checked.

### Shapiro-Wilk residual-normality test

```text
W = 0.3507
p < 0.001
```

Residual normality was rejected.

### Levene test

```text
p < 0.001
```

Equal variance was rejected.

### Brown-Forsythe test

```text
p < 0.001
```

Equal variance was also rejected using the median-centered test.

Because the parametric assumptions were substantially violated, **ordinary factorial ANOVA was not used as the primary inferential method**.

The primary significance analysis used:

```text
Aligned Rank Transform (ART) ANOVA
```

---

# 11. ART ANOVA Results for Energy

At:

```text
α = 0.05
```

ART ANOVA found statistically significant main effects for:

- processing implementation;
- file format;
- numeric data type;
- categorical representation;
- execution mode.

It also identified statistically significant results for most tested interactions.

One notable exception was:

```text
File Format × Numeric Type
p = 0.916
```

which was **not statistically significant**.

Important ART results included:

| Effect | ART result |
|---|---:|
| Processing Implementation | p < 0.001 |
| File Format | p < 0.001 |
| Numeric Data Type | p < 0.001 |
| Categorical Representation | p < 0.001 |
| Execution Mode | p < 0.001 |
| Implementation × File Format | p < 0.001 |
| Implementation × Numeric Type | p < 0.001 |
| Implementation × Category | p < 0.001 |
| Implementation × Execution Mode | p < 0.001 |
| File Format × Numeric Type | **p = 0.916, not significant** |

Statistical significance alone was not used to judge practical importance. Effect sizes were evaluated separately.

---

# 12. Practical Effect Sizes

Partial eta-squared from the original-scale model was used to estimate practical importance.

Selected effects were:

| Effect | Partial η² | Interpretation |
|---|---:|---|
| **Processing Implementation** | **0.985** | Large |
| **Implementation × Category** | **0.973** | Large |
| **Categorical Representation** | **0.924** | Large |
| **Implementation × File Format** | **0.683** | Large |
| Implementation × File Format × Category | 0.406 | Large |
| **File Format** | **0.286** | Large |
| File Format × Category | 0.174 | Large |
| **Implementation × Execution Mode** | **0.065** | Medium |
| Implementation × Numeric Type | 0.027 | Small |
| Block | 0.017 | Small |
| Execution Mode | 0.012 | Small |
| **Numeric Data Type** | **0.005** | Very small |

### Main statistical conclusion

Processing implementation was the strongest practical influence on energy consumption.

More importantly, the very large:

```text
Implementation × Category effect
partial η² = 0.973
```

shows that categorical representation behaves very differently depending on the processing implementation.

This is why configuration choices should be evaluated as combinations rather than isolated optimization rules.

---

# 13. ART-C Post-Hoc Results

ART-C comparisons with Holm correction were used to investigate selected significant effects.

## File Format within each implementation

CSV versus Parquet was statistically significant after Holm correction for **all eight implementations**.

However, the direction was not universal:

- Parquet was favored for most implementations.
- Python Loops showed the opposite pattern.

This confirms the strong **Implementation × File Format** interaction.

## Numeric Type within each implementation

After Holm correction, Float32 versus Float64 was statistically significant for only some implementations:

- Pandas NumPy: significant
- Polars Lazy: significant
- Python Loops: significant
- Dask Default: not significant
- Dask PyArrow: not significant
- DuckDB: not significant
- Pandas PyArrow: not significant
- Polars Eager: not significant

This supports the conclusion that numeric precision had a comparatively small and inconsistent practical effect.

## Categorical Representation within each implementation

Category versus Object was statistically significant for most implementations after Holm correction.

Notably:

- Pandas NumPy showed lower energy with Category.
- Pandas PyArrow showed lower energy with Category.
- Dask Default showed much higher energy with Category.
- Dask PyArrow showed much higher energy with Category.
- Polars Lazy did not show a significant category/object difference after Holm correction.

This is one of the clearest examples of an implementation-dependent optimization.

## Execution Mode within each implementation

Parallel versus Single was statistically significant after Holm correction for most implementations.

Notably:

- Polars Eager: parallel reduced energy
- Polars Lazy: parallel reduced energy
- DuckDB: parallel reduced energy
- Pandas NumPy: parallel increased energy
- Pandas PyArrow: parallel increased energy
- Python Loops: parallel increased energy
- Dask PyArrow: difference was not significant after Holm correction

Again, the result shows that enabling parallel execution is not universally beneficial for energy.

---

# 14. Energy and Runtime Relationship

Energy and runtime were very strongly correlated.

### Run level

```text
Pearson r = 0.994
Spearman ρ = 0.990
```

### Configuration-mean level

```text
Pearson r = 0.999
Spearman ρ = 0.989
```

This means that configurations taking longer to execute generally also consumed more processor-related energy.

However, runtime should not be treated as a complete substitute for direct energy measurement because power use can also differ across implementations.

---

# 15. Peak Memory Result

The lowest observed configuration-level mean peak memory was approximately:

```text
519.73 MB
```

for:

```text
Pandas PyArrow
+ CSV
+ Float32
+ Category
+ Single
```

This was **not** the lowest-energy configuration.

Therefore, the configuration that minimizes energy is not necessarily the configuration that minimizes memory.

This supports treating:

- energy;
- runtime;
- memory

as related but distinct optimization objectives.

---

# 16. Pareto Analysis

Pareto analysis was used to identify configurations that could not be improved in one objective without worsening another considered objective.

## Energy + Runtime

Only **one configuration** was Pareto-efficient:

```text
Polars Eager
+ Parquet
+ Float32
+ Object
+ Parallel
```

This means no other tested configuration had both:

- lower or equal energy, and
- lower or equal runtime,

with a strict improvement in at least one of the two objectives.

## Energy + Runtime + Peak Memory

When peak memory was added as a third objective:

```text
10 configurations
```

were Pareto-efficient.

This occurs because several configurations that are worse in energy or runtime provide better memory usage.

---

# 17. Randomized-Block Robustness

The measurements were collected across **10 randomized blocks**.

The block effect on energy was small:

```text
partial η² = 0.017
```

This indicates that block-to-block variation was much smaller than the major software-configuration effects.

---

# 18. Position-in-Block Drift

To check whether later runs systematically consumed more or less energy, the relationship between run position and residual energy was tested.

```text
Spearman ρ = 0.039
p = 0.168
```

The relationship was not statistically significant.

This provides no evidence of a meaningful systematic run-position drift in the residual energy measurements.

---

# 19. Leave-One-Block-Out Robustness

The configuration ranking was recalculated **10 times**, each time excluding one randomized block.

The full-sample best configuration:

```text
Polars Eager
+ Parquet
+ Float32
+ Object
+ Parallel
```

remained:

```text
Rank 1 in 10 out of 10 leave-one-block-out analyses
```

Therefore, the main ranking result was not dependent on any single randomized block.

---

# 20. Carbon Estimate

The study converts measured processor-related energy to a location-based operational carbon estimate using the Bangladesh grid emission factor:

```text
0.62 kg CO₂/kWh
```

The conversion is:

```text
CO₂ = Energy(J) / 3,600,000 × 0.62
```

Equivalent conversion:

```text
≈ 0.172 mg CO₂ per Joule
```

Selected estimates:

| Result | Energy | Estimated CO₂ |
|---|---:|---:|
| Lowest-energy configuration | 8.98 J | **1.55 mg/run** |
| Overall median | 31.49 J | **5.42 mg/run** |
| Overall mean | 124.86 J | **21.50 mg/run** |
| Highest-energy configuration | 637.88 J | **109.86 mg/run** |

Across all 1,280 measured runs:

```text
≈ 159.81 kJ
≈ 0.0444 kWh
≈ 27.52 g CO₂
```

These values are **location-based operational estimates derived from processor-related RAPL energy**.

They are **not** whole-system electricity measurements and do not represent life-cycle carbon emissions.

---

# 21. Answers to the Research Questions

## Main RQ

**To what extent do processing implementation, file format, numeric data type, categorical representation, and execution mode influence energy consumption?**

Processing implementation had the strongest practical effect:

```text
partial η² = 0.985
```

Categorical representation and file format also had important effects, but their impacts depended strongly on processing implementation.

Numeric data type had only a very small practical main effect.

Therefore, energy consumption depends primarily on the **complete software configuration**, rather than one isolated setting.

---

## Sub-RQ1

**Which combination resulted in the lowest energy consumption?**

```text
Polars Eager
+ Parquet
+ Float32
+ Object
+ Parallel
```

Result:

```text
Mean energy = 8.98 J
95% CI = 8.67–9.30 J
Mean runtime = 0.79 s
```

It remained the best-ranked configuration in all 10 leave-one-block-out analyses.

---

## Sub-RQ2

**How do the tested factors and their interactions affect energy consumption?**

The results show strong implementation-dependent effects.

The largest selected interaction was:

```text
Implementation × Category
partial η² = 0.973
```

Other important patterns include:

- Parquet generally lowered energy, but not for Python Loops.
- Category reduced energy in Pandas but greatly increased it in Dask.
- Parallel execution helped Polars and DuckDB but increased energy for Pandas and Python Loops.
- Float32 versus Float64 had comparatively small and inconsistent effects.

Therefore, common optimization choices cannot be treated as universal rules.

---

## Sub-RQ3

**Are the energy differences statistically significant and practically meaningful?**

Yes, for the major factors and interactions.

ART ANOVA identified significant main effects and most interactions at:

```text
α = 0.05
```

The File Format × Numeric Type interaction was a notable exception:

```text
p = 0.916
```

Effect-size analysis showed very large practical effects for:

```text
Processing Implementation       partial η² = 0.985
Implementation × Category       partial η² = 0.973
Categorical Representation      partial η² = 0.924
Implementation × File Format    partial η² = 0.683
```

Therefore, the major observed differences were not only statistically significant but also practically substantial.

---

# 22. Main Conclusion

The central result of the study is:

> **Energy-efficient Python data processing is configuration-dependent and interaction-dependent.**

There is no single optimization choice that is always best.

Instead, the effect of:

- file format;
- numeric type;
- categorical representation;
- execution mode

can depend on the processing implementation with which it is combined.

For the tested workload, the best overall configuration was:

```text
Polars Eager + Parquet + Float32 + Object + Parallel
```

The practical implication is that developers should evaluate **complete software configurations** instead of applying universal rules such as:

```text
Always use Parquet
Always use Category
Always use Float32
Always enable parallel execution
```

---

# 23. Important Validation Note

The benchmark included output-consistency checks using row counts and stored result hashes.

A total of:

```text
21 configurations
```

produced more than one stored output hash across repeated runs.

Possible explanations include:

- output row ordering;
- floating-point serialization;
- implementation-specific output representation.

These configurations were retained and flagged rather than removed.

Therefore, exact functional-equivalence claims for these flagged cases should be supported by direct numerical-equivalence testing with deterministic ordering and an appropriate floating-point tolerance.

This issue does **not** change the measured benchmark values, but it is an important reproducibility and validation limitation.

---

# 24. Reproducing the Results

Open:

```text
Data_Analysis.ipynb
```

and run the notebook from top to bottom using the benchmark dataset.

The notebook reproduces:

- overall descriptive statistics;
- configuration rankings;
- implementation summaries;
- factor-level summaries;
- interaction plots;
- original-scale factorial model;
- assumption tests;
- ART ANOVA;
- ART-C post-hoc comparisons;
- Holm-adjusted significance tests;
- partial eta-squared effect sizes;
- energy-runtime correlations;
- average power;
- energy-delay product;
- Pareto analysis;
- block diagnostics;
- position-in-block drift analysis;
- leave-one-block-out robustness analysis.

---

# 25. Suggested Repository Files

```text
.
├── README.md
├── Data_Analysis.ipynb
├── benchmark_test.csv
├── figures/
│   ├── implementation_category_interaction.png
│   └── energy_runtime_pareto.png
├── results/
│   ├── configuration_summary_and_ranking.csv
│   ├── engine_summary.csv
│   ├── energy_exact_ART_ANOVA.csv
│   ├── energy_ARTC_engine_pairwise_Holm.csv
│   ├── energy_ARTC_engine_by_format_selected_within_engine_Holm.csv
│   ├── energy_ARTC_engine_by_numeric_selected_within_engine_Holm.csv
│   ├── energy_ARTC_engine_by_category_selected_within_engine_Holm.csv
│   ├── energy_ARTC_engine_by_execution_selected_within_engine_Holm.csv
│   ├── pareto_analysis.csv
│   └── leave_one_block_out_robustness.csv
└── paper/
    └── paper.pdf
```

---

# 26. Citation

If you use the analysis or results, please cite the associated paper:

```bibtex
@techreport{maisha2026carboncost,
  title = {The Carbon Cost of Everyday Python: A Reproducible Study of Energy-Efficient Data Processing Practices},
  author = {Maisha, Atkiya and Ferdous Naba, Jannatul and Sabah Bushra, Jarin and Tuhin, Rashedul Amin},
  year = {2026},
  institution = {East West University}
}
```

