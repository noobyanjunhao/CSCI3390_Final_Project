package final_project

import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.util.Random
import scala.util.control.Breaks._

object TwitterPivotClustering {

  // ------------------------------------------------------------------
  // Vertex data and message types for Pregel pivot clustering
  // ------------------------------------------------------------------
  final case class VData(cluster: Long, key: Double, active: Boolean)
  final type Msg = (Long, Double)

  // ------------------------------------------------------------------
  // Entry point
  // ------------------------------------------------------------------
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: spark-submit ... TwitterPivotClustering <in.csv> <outDir>")
      sys.exit(1)
    }
    val Array(inPath, outPath) = args

    // ─── Spark set-up ──────────────────────────────────────────────────────
    val spark = SparkSession.builder()
      .appName("TwitterPivotClustering")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val t0 = System.currentTimeMillis()

    // ─── Read positive edges and build graph ───────────────────────────────
    println(s"Reading input from: $inPath")
    val edges = sc.textFile(inPath)
      .map(_.split(','))
      .map { case Array(a, b) =>
        val (u, v) = (a.toLong, b.toLong)
        if (u < v) Edge(u, v, 1) else Edge(v, u, 1)
      }
      .persist()
    val graph = Graph.fromEdges(edges, defaultValue = 0)

    val numV = graph.vertices.count()
    val numE = edges.count()
    println(s"Loaded graph with $numV vertices and $numE edges")

    // ─── Multiple‐seed pivot clustering ─────────────────────────────────────
    println("Starting pivot clustering (10 seeds)...")
    val runs = 10
    val seeds = (1 to runs).map(_ => Random.nextLong())
    var bestClust: RDD[(VertexId, Long)] = null
    var bestObj = Long.MaxValue

    seeds.zipWithIndex.foreach { case (seed, i) =>
      println(s">>> Pivot run ${i+1}/$runs  (seed=$seed)")
      Random.setSeed(seed)

      val clustering = runPivotClustering(graph)
      val obj = countDisagreements(graph, clustering)
      println(f"    disagreements = $obj%,d")

      if (obj < bestObj) {
        bestObj = obj
        bestClust = clustering
        println("    → new best!")
      }
    }
    println(f"Best pivot result: $bestObj%,d disagreements")

    // ─── Twitter‐tuned local‐move refinement ────────────────────────────────
    println("Applying local‐move refinement (up to 3 sweeps)...")
    val refined = localMoveRefinement(graph, bestClust, maxSweeps = 3)
    val finalObj = countDisagreements(graph, refined)
    println(f"After refinement: $finalObj%,d disagreements")

    // ─── Save final clustering ─────────────────────────────────────────────
    println(s"Saving clusters to: $outPath")
    refined
      .map { case (v, c) => s"$v,$c" }
      .sortBy(identity)
      .coalesce(1)
      .saveAsTextFile(outPath)

    val totalTime = (System.currentTimeMillis() - t0) / 1000.0
    println(f"Total runtime: $totalTime%.1f seconds")
    spark.stop()
  }

  // ------------------------------------------------------------------
  // 1) Pivot clustering via Pregel
  // ------------------------------------------------------------------
  def runPivotClustering(graph: Graph[Int,Int]): RDD[(VertexId, Long)] = {
    // initialize each vertex with a random key
    val rnd = new Random()
    val initial: Graph[VData, Int] = graph.mapVertices { case (id, _) =>
      VData(cluster = -1L, key = rnd.nextDouble(), active = true)
    }
    // Pregel loop: vertices send their key to active neighbours to find pivots
    val pivoted = initial.pregel(
      initialMsg    = (-1L, Double.PositiveInfinity),
      maxIterations = 30,
      activeDirection = EdgeDirection.Either
    )(
      // vertexProgram
      (id, data, msg) => {
        var VData(c, k, act) = data
        val (bestPid, bestKey) = msg
        if (c == -1L && bestPid != -1L) {
          // if I'm the chosen pivot, or claimed by a pivot, I join that cluster
          c = bestPid
          act = false
        }
        VData(c, k, act)
      },
      // sendMessage
      triplet => {
        val buf = scala.collection.mutable.ArrayBuffer.empty[(VertexId, Msg)]
        val s = triplet.srcAttr; val d = triplet.dstAttr
        if (s.active && d.active) {
          if (s.key < d.key) buf += ((triplet.dstId,  (triplet.srcId, s.key)))
          else if (d.key < s.key) buf += ((triplet.srcId, (triplet.dstId, d.key)))
        }
        buf.iterator
      },
      // mergeMessage
      (m1, m2) => if (m1._2 < m2._2) m1 else m2
    )
    // extract final cluster assignments
    pivoted.vertices.map { case (vid, VData(c,_,_)) =>
      val cid = if (c == -1L) vid else c
      (vid, cid)
    }
  }

  // ------------------------------------------------------------------
  // 2) Count disagreements: pos-across + neg-inside
  // ------------------------------------------------------------------
  def countDisagreements(
      graph: Graph[Int,Int],
      clustering: RDD[(VertexId, Long)]
    ): Long = {

    val sc = graph.vertices.sparkContext
    val cmap = sc.broadcast(clustering.collectAsMap())

    // pos-across
    val posAcross = graph.triplets.filter { t =>
      cmap.value(t.srcId) != cmap.value(t.dstId)
    }.count()

    // sizes per cluster
    val sizes = clustering.map { case (_, c) => (c, 1L) }
                          .reduceByKey(_ + _)
                          .collectAsMap()
    val sbc = sc.broadcast(sizes)

    // pos-inside per cluster
    val posInside = graph.triplets.filter { t =>
      cmap.value(t.srcId) == cmap.value(t.dstId)
    }.map(t => (cmap.value(t.srcId), 1L))
     .reduceByKey(_ + _)
     .collectAsMap()

    // neg-inside = totalPossible - posInside
    val negInside = sbc.value.map { case (c, sz) =>
      val p = posInside.getOrElse(c, 0L)
      sz*(sz-1)/2 - p
    }.sum

    posAcross + negInside
  }

  // ------------------------------------------------------------------
  // 3) Twitter‐tuned greedy local‐move refinement
  // ------------------------------------------------------------------
  /**
   *  • processes vertices in ascending ID order
   *  • skips any v with degree > maxDeg
   *  • uses full Δ = pos_gain - neg_newInside - neg_removed
   */
  def localMoveRefinement(
      g: Graph[Int,Int],
      clustering: RDD[(VertexId,Long)],
      maxSweeps: Int
    ): RDD[(VertexId,Long)] = {

    val sc = clustering.sparkContext
    val degreeMap = g.degrees.map{ case (id, deg) => (id, deg.toLong) }.collectAsMap()
    val degOf = sc.broadcast(degreeMap)
    val maxDeg    = 50000L

    var current = clustering.persist()
    var bestObj = countDisagreements(g, current)
    println(s"  start objective = $bestObj")

    var sweep = 1
    var improved = true
    while (sweep <= maxSweeps && improved) {

      // broadcast current cluster lookup & sizes
      val cidOf  = sc.broadcast(current.collectAsMap())
      val sizeOf = sc.broadcast(
        current.map { case (_, c) => (c,1L) }
               .reduceByKey(_+_)
               .collectAsMap()
      )

      // gather (v → Seq[(tgtC, posCount)])
      val posTo = g.triplets.flatMap { t =>
        if (t.attr > 0) {
          val cs = cidOf.value(t.srcId)
          val cd = cidOf.value(t.dstId)
          Iterator((t.srcId, cd), (t.dstId, cs)).map(vc => (vc, 1L))
        } else Iterator.empty
      }
      .reduceByKey(_+_)
      .map { case ((v,c),cnt) => (v, (c,cnt)) }
      .groupByKey()
      .sortByKey()  // enforce deterministic order

      // propose best move for each v
      val moves = posTo.rightOuterJoin(current).map { case (v, (optIt, curC)) =>
        if (degOf.value.getOrElse(v, 0L) > maxDeg) {
          (v, curC)
        } else {
          val nb   = optIt.getOrElse(Seq.empty).toMap.withDefaultValue(0L)
          val pCur = nb(curC)
          val sCur = sizeOf.value(curC)

          var bestC = curC
          var bestΔ = 0L
          nb.keysIterator.foreach { tgtC =>
            val pToT   = nb(tgtC)
            val sT     = sizeOf.value(tgtC)
            val negNew = sT - pToT
            val negOld = sCur - pCur
            val Δ      = pToT - negNew - negOld
            if (Δ < bestΔ) {
              bestΔ = Δ; bestC = tgtC
            }
          }
          (v, bestC)
        }
      }.persist()

      // apply moves
      val candidate = current
        .leftOuterJoin(moves)
        .mapValues {
          case (oldC, Some(nC)) => nC
          case (oldC, None)     => oldC
        }
        .persist()

      val candObj = countDisagreements(g, candidate)
      println(s"  sweep $sweep objective = $candObj")

      if (candObj < bestObj) {
        current.unpersist(false)
        current = candidate
        bestObj = candObj
        sweep += 1
      } else {
        candidate.unpersist(false)
        improved = false
      }
    }

    current
  }
}
