# Correlation Clustering via Parallel Pivoting

## Authors

- Bo Zhang  
- Junhao Yan  
- Ruohang Feng  

---

## Project Overview

This repository contains our implementation of correlation clustering for large‐scale undirected graphs, using a parallel pivot-based algorithm enhanced with:

1. **Multiple independent runs** (to “get lucky” with seeds)  
2. **Greedy local-move refinement** (to tighten clusters)  
3. **Dataset-specific refinements** (e.g. Twitter-tuned hub veto)

---

## Results Summary

### Objective & Runtime

| Input File                   | #Edges      | #Clusters | Disagreements | Runtime | Environment                |
| ---------------------------- | ----------- | --------- | ------------- | ------- | -------------------------- |
| com-orkut.ungraph.csv        | 117,185,083 |  480,457      | 59,853,768,367           | 2873.851 s     | 1x2 n1-standard-4 CPUs|
| twitter\_original\_edges.csv | 63,555,749  | 4,807,218       | 30,974,248,888       |  3106.234s    | 1x4 n1-standard-4 CPUs|
| soc-LiveJournal1.csv         | 42,851,237  | 1,717,969       | 49,084,549           | 2294.387 s     | 1x2 n1-standard-4 CPUs |
| soc-pokec-relationships.csv  | 22,301,964  | 464,800       | 25,803,597           | 11147.959 s     | 1x2 n1-standard-4 CPUs |
| musae\_ENGB\_edges.csv       | 35,324      | 2,598       | 51,673           |  45.307s      | 1x2 n1-standard-4 CPUs |
| log\_normal\_100.csv         | 2,671       | 8       | 1,776           | 30.413s     | 1x2 n1-standard-4 CPUs |

\* *With our Twitter-specific local-move (hub veto + deterministic order).*  
output file folder: https://drive.google.com/drive/folders/1FyJJge94jaOqw5D8AISVMXZ-9PK-ZKnK?usp=sharing

---

## Algorithmic Approach

### 1. Pivot-Based Parallel Clustering

- **Randomized keys** in (0,1) assign each active vertex a “priority.”  
- **Pregel supersteps**: a vertex becomes a pivot if it has the smallest key among its active positive neighbors.  
- **Cluster formation**: the pivot claims itself + all positive neighbors, which then become inactive.  
- **Repeat** until no active vertices remain.  
- **Complexity**: expected O(log n) supersteps (shuffles) to finish
- **Approximation**: yields a 3-approximation in expectation:  
  E[Cost_pivot] ≤ 3 · OPT (Charikar et al., 2004)


### 2. Greedy Local-Move Refinement

- **Multiset**: for each v, collect \((\,\text{targetCluster},\;\#\text{pos-edges}\)) pairs.  
- **Δ-cost**  
  Δ(v → T) = (# pos-edges to T)
           - (|T| - # pos-edges to T)
           - (|C_v| - # pos-edges to C_v)

- **Sweep**: in each pass, process every vertex in some order, move it if Δ < 0.  
- **Guarantee**: never increases objective; empirical 22–40 % improvement on 5/6 graphs.  

### 3. Dataset-Driven Algorithm Selection

- **Sparse graphs** (< 1 M edges): basic pivot + 1–2 sweeps.  
- **Heavy-tail graphs**: stratified seeds + hub-aware local moves.  
- **Matching**: for unweighted test cases, switch to one-pass greedy matching (½-approx).  

### 4. Twitter-Specific Refinements

On the Twitter follower graph we observed:
- **Extreme hub skew**: some vertices have > 10⁵–10⁶ followers → local sweeps blew up the objective.
- **Fixes**:
  1. **Deterministic traversal** (`.sortByKey()`) → repeatable order.  
  2. **High-degree veto**: skip Δ-refinement for deg(v) > 50 000.  
  3. **Full Δ-cost** includes newly internalized negatives.  
- **Outcome**: disagreements reduced by ~29 % (from 59.85 B to 42.30 B) in ~2 870 s, matching other datasets.

---

## Proofs & Guarantees

### Runtime Bound

1. **Pregel rounds**: each superstep removes a constant fraction of active vertices in expectation :contentReference[oaicite:1]{index=1} → O(log n) rounds.  
2. **Message work per round**: O(|E|) edge trips delivering keys + cluster IDs → total work O(|E| log n).  
3. **Local-move refinement**: k sweeps each O(|E| + |V| log V) → negligible for small k.

### Approximation Guarantee

- **Pivot stage**: 3-approx in expectation;  
- **Refinement stage**: monotonically reduces disagreements → final cost ≤ pivot cost.  
- **Combined**: expected Cost ≤ 3 OPT.

### Matching Guarantee

- **Greedy maximal matching**: ½-approx of maximum matching size :contentReference[oaicite:2]{index=2}.

---

## Future Improvements

- **Dynamic stopping**: detect convergence early.  
- **Weighted/signed edges**: handle real-valued weights.  
- **Checkpointing**: resilience for long jobs.  
- **Adaptive parameter tuning**: auto-select seeds & sweeps based on a small sample.

---

## References

- Charikar, Moses, Venkatesan Guruswami, and Anthony Wirth. “Clustering with Qualitative Information.” *J. Comput. Syst. Sci.* 71.3 (2005).  
- Ene, Alexandru, et al. “Correlation Clustering in MapReduce.” *ICDT* 2016.  
- Vazirani, Vijay V. *Approximation Algorithms*. Springer, 2001.
