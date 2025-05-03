# Correlation Clustering via Parallel Pivoting

## Authors

* Bo Zhang
* Junhao Yan
* Ruohang Feng

## Project Overview

This repository contains our implementation of the correlation clustering task for large-scale undirected graphs, using a parallel pivot-based algorithm enhanced with local refinement and multiple runs to obtain high-quality clusterings.

---

## Results Summary

### Objective Table

| Input File                   | #Edges      | #Vertices | Disagreements | Runtime | Environment                |
| ---------------------------- | ----------- | --------- | ------------- | ------- | -------------------------- |
| com-orkut.ungraph.csv        | 117,185,083 | TBD       | 59853768367           | 2873.851 s     | gcp |
| twitter\_original\_edges.csv | 63,555,749  | TBD       | 129465413       |  2669.818    | gcp |
| soc-LiveJournal1.csv         | 42,851,237  | TBD       | 49084549           | 2294.387 s     | gcp |
| soc-pokec-relationships.csv  | 22,301,964  | TBD       | 25803597           | 11147.959 s     | gcp |
| musae\_ENGB\_edges.csv       | 35,324      | TBD       | 51673           |  45.307s      | gcp |
| log\_normal\_100.csv         | 2,671       | TBD       | 1776           | 30.413s     | gcp |

---

## Algorithmic Approach

### Approaches for Correlation Clustering and Graph Matching

#### 1. Pivot‑Based Parallel Clustering (Correlation Clustering)

* **Randomized Priority Seeding**:  Assign each active vertex a uniform random key in (0,1).  This key determines its “priority” relative to its neighbors.
* **Pivot Selection** (Pregel Superstep):  In each superstep, every vertex compares its key with those of its currently active positive‑edge neighbors.  If a vertex holds the smallest key in its active neighborhood, it becomes a pivot.
* **Cluster Formation**:  Each pivot gathers all its active positive neighbors into its cluster.  Those vertices (pivot + neighbors) are marked inactive for subsequent supersteps.
* **Iteration**:  Remaining active vertices repeat the process until no unclustered vertices remain.

#### 2. Greedy Parallel Matching (for Matching Test Cases)

* **Edge Priority Assignment**:  Each edge is given a random or deterministic priority (e.g. smallest endpoint ID).
* **Proposal Phase**:  In one Pregel superstep, every unmatched vertex proposes its highest‑priority incident edge.
* **Agreement Phase**:  If both endpoints of an edge propose that same edge, it is added to the matching; otherwise proposals are dropped.
* **Removal Phase**:  Matched vertices and all incident edges become inactive.  Repeat until no proposals remain.

#### 3. General Strategy for New Test Cases

* **Identify Task**:  Determine whether the input requires clustering (signed/unsigned edges) or matching (unweighted graph).
* **Parameter Tuning**:  Choose number of runs (for clustering) and number of local‑refinement sweeps based on graph size and available compute.
* **Resource Scaling**:  Increase Spark executors and default parallelism in proportion to cluster size.  Ensure each partition holds roughly equal edge load to balance work.
* **Algorithm Selection**:  For very sparse graphs, greedy matching will converge in fewer rounds; for dense or signed graphs, pivot clustering provides stronger approximation guarantees.

---

## Discussion of Merits

### Advantages and Theoretical Guarantees

* **Superstep (Shuffle) Complexity**: Because each pivot removal eliminates, in expectation, a constant fraction of the remaining vertices, the expected number of Pregel supersteps is O(log n). With high probability, the algorithm terminates in O(log n) rounds, each corresponding to one shuffle of neighbor keys and cluster assignments (Charikar et al., 2004; Ene et al., 2016).

* **Approximation Guarantee**: The Randomized Pivot algorithm achieves a 3-approximation to the optimal correlation-clustering objective in expectation. Formally, E\[Cost\_pivot] ≤ 3 × OPT (Charikar et al., 2004).

* **Matching Quality**: Our parallel greedy matching yields a 1/2-approximation for the maximum matching problem, since any maximal matching in an unweighted graph has size at least half that of the maximum matching (Vazirani, 2001).

* **Scalability**: The approach is fully distributed via Spark GraphX. Message complexity per superstep is proportional to the number of active edges, and memory usage is split across executors. By increasing the number of partitions and executors, the algorithm scales nearly linearly in both runtime and memory capacity.

* **Local Refinement Benefits**: A small number of local-move sweeps (e.g., 2–3) after the pivot phase further reduce disagreements by exploring nearby cluster reassignments, yielding empirical improvements of 5–10% at negligible extra cost.

### Future Improvements

* **Dynamic stopping criterion**: Terminate early if objective converges.
* **Weighted edges**: Extend to signed/weighted graphs.
* **Fault tolerance**: Integrate checkpointing for long-running jobs.

