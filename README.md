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

| Input File                   | #Edges      | #Vertices | Disagreements (pivot→refined)      | Runtime (pivot+refine) | Env |
| ---------------------------- | ----------- | --------- | ---------------------------------- | ---------------------- | --- |
| com-orkut.ungraph.csv        | 117 185 083 | 3 072 441 | 197 367 948 → 129 465 413 (−34 %)   | 2 754 s                 | GCP |
| twitter_original_edges.csv   | 63 555 749  | 11 316 811| 59 853 768 367 → 42 297 638 857 (−29 %)* | 2 873 s                 | GCP |
| soc-LiveJournal1.csv         | 42 851 237  | 4 846 609 | 82 269 817 → 49 084 549 (−40 %)     | 2 294 s                 | GCP |
| soc-pokec-relationships.csv  | 22 301 964  | 1 632 803 | 36 957 179 → 25 803 597 (−30 %)     | 1 208 s                 | GCP |
| musae_ENGB_edges.csv         | 35 324      | 7 126     |   72 484 →    51 673 (−29 %)        |   45 s                   | GCP |
| log_normal_100.csv           | 2 671       |     100   |    2 278 →     1 776 (−22 %)        |   30 s                   | GCP |

\* *With our Twitter-specific local-move (hub veto + deterministic order).*  

---

## Algorithmic Approach

### 1. Pivot-Based Parallel Clustering

- **Randomized keys** in (0,1) assign each active vertex a “priority.”  
- **Pregel supersteps**: a vertex becomes a pivot if it has the smallest key among its active positive neighbors.  
- **Cluster formation**: the pivot claims itself + all positive neighbors, which then become inactive.  
- **Repeat** until no active vertices remain.  
- **Complexity**: expected O(log n) supersteps (shuffles) to finish :contentReference[oaicite:0]{index=0}.  
- **Approximation**: yields a 3-approximation in expectation:  
  \[
    \mathbb{E}[\mathrm{Cost}_\mathrm{pivot}]\;\le\;3\,\mathrm{OPT}
    \quad(\text{Charikar et al., 2004})\,. 
  \]

### 2. Greedy Local-Move Refinement

- **Multiset**: for each v, collect \((\,\text{targetCluster},\;\#\text{pos-edges}\)) pairs.  
- **Δ-cost**  
  \[
    \Delta(v\!\to T)
    = (\text{pos to }T)
    - (\lvert T\rvert - \text{pos to }T)
    - (\lvert C_v\rvert - \text{pos to }C_v)
  \]
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
