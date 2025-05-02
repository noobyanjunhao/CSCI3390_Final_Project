# Correlation Clustering via Parallel Pivoting

## Authors

* Bo Zhang
* Junhao Yan
* Ruohang Feng

## Project Overview

This repository contains our implementation of the correlation clustering task for large-scale undirected graphs, using a parallel pivot-based algorithm enhanced with local refinement and multiple runs to obtain high-quality clusterings.

## Deliverables

1. **Output files**: For each provided input file `XXX.csv`, the corresponding solution file is named `XXX_solution.csv`. All output files should be compressed into a single archive (`solutions.zip`) or, if too large, hosted on Google Drive with a sharing link provided below:

   * Google Drive link: \[TBD]

2. **Source code**: All source code is located under `src/main/scala/final_project/`, including:

   * `PivotClustering.scala` — our parallel pivot clustering implementation
   * `clustering_verifier.scala` — verifier to compute disagreements on a clustering

3. **Report**: This `README.md` file serves as the project report, covering:

   * Results summary (objectives and runtimes)
   * Computational environment
   * Algorithmic approach and enhancements
   * Discussion of theoretical and practical merits

4. **Presentation slides**: `presentation.pdf` (to be added prior to class)

---

## Repository Structure

```
├── data/
│   ├── com-orkut.ungraph.csv
│   ├── twitter_original_edges.csv
│   ├── soc-LiveJournal1.csv
│   ├── soc-pokec-relationships.csv
│   ├── musae_ENGB_edges.csv
│   └── log_normal_100.csv
├── src/
│   └── main/scala/final_project/
│       ├── PivotClustering.scala
│       └── clustering_verifier.scala
├── solutions.zip         # Compressed output files (or link to Drive)
├── presentation.pdf      # Slides for 10-minute class presentation
├── build.sbt             # SBT build configuration
└── README.md             # Project report (this file)
```

## Environment and Dependencies

* **Scala** 2.12
* **Apache Spark** 3.x with GraphX
* **SBT** for build and packaging
* **Google Cloud Platform** n1-standard-2 instances (2 vCPUs, 7.5 GB RAM)

### Setup

```bash
# Compile and package
sbt clean package

# Run pivot clustering on one dataset (example)
spark-submit \
  --master local[*] \
  --class final_project.PivotClustering \
  target/scala-2.12/project_3_2.12-1.0.jar \
  data/log_normal_100.csv \
  output/log_normal_100_solution

# Verify clustering disagreements
spark-submit \
  --master local[*] \
  --class final_project.clustering_verifier \
  target/scala-2.12/project_3_2.12-1.0.jar \
  data/log_normal_100.csv \
  output/log_normal_100_solution/part-00000
```

---

## Results Summary

### Objective Table

| Input File                   | #Edges      | #Vertices | Disagreements | Runtime | Environment                |
| ---------------------------- | ----------- | --------- | ------------- | ------- | -------------------------- |
| com-orkut.ungraph.csv        | 117,185,083 | TBD       | TBD           | TBD     | GCP n1-standard-2 (2 vCPU) |
| twitter\_original\_edges.csv | 63,555,749  | TBD       | TBD           | TBD     | GCP n1-standard-2 (2 vCPU) |
| soc-LiveJournal1.csv         | 42,851,237  | TBD       | TBD           | TBD     | GCP n1-standard-2 (2 vCPU) |
| soc-pokec-relationships.csv  | 22,301,964  | TBD       | TBD           | TBD     | GCP n1-standard-2 (2 vCPU) |
| musae\_ENGB\_edges.csv       | 35,324      | TBD       | TBD           | TBD     | GCP n1-standard-2 (2 vCPU) |
| log\_normal\_100.csv         | 2,671       | TBD       | TBD           | TBD     | GCP n1-standard-2 (2 vCPU) |

*Fill in the placeholders above with actual values from your experiments.*

---

## Computational Resources

All experiments were conducted on Google Cloud Platform using two n1-standard-2 instances (2 vCPUs, 7.5 GB RAM each). Spark was configured with 8 executors and default parallelism set to 8. Total memory per executor: 3 GB.

---

## Algorithmic Approach

### 1. Parallel Pivot Clustering

* **Randomized seeding**: Assign each active vertex a random key in (0,1).
* **Pivot selection**: In each superstep (Pregel), the vertex with the smallest key among its positive neighbors becomes a pivot and forms a cluster with its active neighbors.
* **Iterative removal**: Remove clustered vertices and repeat until all vertices are assigned.
* **Approximation guarantee**: 3-approximation for correlation clustering (Charikar et al., 2004).

### 2. Multiple Runs and Best Selection

* Perform *N* independent runs (e.g., 10) with different random seeds.
* Compute disagreements for each run and select clustering with minimum disagreements.

### 3. Local Move Refinement

* Apply a greedy local-move heuristic for a fixed number of sweeps (e.g., 3).
* For each vertex, evaluate the objective change when moving to neighboring clusters and accept moves that reduce disagreements.

### 4. Output Formatting

* Final clustering `(vertexID, clusterID)` pairs sorted by vertex ID.
* Coalesce to a single CSV file for each dataset.

---

## Discussion of Merits

* **Theoretical guarantee**: 3-approximation bound for the pivot algorithm under positive-edge-only model.
* **Scalability**: Leverages Spark GraphX and Pregel API for distributed message passing; runs in *O*(maxIterations) supersteps.
* **Parallelism**: Each iteration communicates only local neighbor information, reducing shuffle.
* **Refinement**: Local moves improve empirical performance without breaking approximation.

### Future Improvements

* **Dynamic stopping criterion**: Terminate early if objective converges.
* **Weighted edges**: Extend to signed/weighted graphs.
* **Fault tolerance**: Integrate checkpointing for long-running jobs.

---

## Presentation

A 10-minute presentation covering the above will be delivered in class on **April 29** and **May 1**. The slides (`presentation.pdf`) will outline:

1. Project goal
2. Algorithmic approach
3. Results and analysis
4. Future directions

---

## References

* Charikar, M., Guruswami, V., & Wirth, A. (2004). *Clustering with Qualitative Information.* Journal of Computer and System Sciences.
* Apache Spark GraphX Programming Guide. Retrieved from [https://spark.apache.org/graphx/](https://spark.apache.org/graphx/)

---

*Last updated: May 2, 2025*
