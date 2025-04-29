// src/main/scala/final_project/PivotClustering.scala
package final_project

import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.util.Random
import scala.util.control.Breaks._

/**
 *  Parallel pivot-based correlation clustering.
 *
 *  How it works (BBC 2004, 3-approximation):
 *    1.  Give every still–unclustered vertex a random priority key in (0,1).
 *    2.  A vertex whose key is the smallest among itself and all POSITIVE neighbours
 *        becomes a pivot and claims all of its positive neighbours.
 *    3.  Remove the new cluster from further consideration and repeat
 *        (the Pregel loop does this for us).
 *  
 *  Enhanced with:
 *    - Multiple runs with different random seeds
 *    - Local move refinement
 *    - Cluster merging with correct disagreement calculation
 */
object PivotClustering {

  final case class VData(cluster: Long, key: Double, active: Boolean) // vertex attr
  final type Msg = (Long, Double) // (bestPivotId, bestPivotKey)

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: spark-submit ... PivotClustering <in.csv> <out.csv>")
      sys.exit(1)
    }
    val (inPath, outPath) = (args(0), args(1))

    // ─── Spark set-up ──────────────────────────────────────────────────────────
    val spark = SparkSession.builder()
      .appName("Correlation-Clustering-Pivot")
      // .master("local[*]")   // uncomment for local debugging
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val startTimeTotal = System.currentTimeMillis()

    // ─── Read the positive edges ──────────────────────────────────────────────
    println(s"Reading input from: $inPath")
    val startTimeLoading = System.currentTimeMillis()
    
    val edges = sc.textFile(inPath)
      .map(_.split(','))
      .map{ case Array(a,b) =>
        val (u,v) = (a.toLong, b.toLong)
        if (u < v) Edge(u,v,1) else Edge(v,u,1)
      }
      .persist()

    val graph = Graph.fromEdges(edges, defaultValue = 0)
    
    // Get all vertices for statistics
    val vertices = graph.vertices.map(_._1).persist()
    val vertexCount = vertices.count()
    val edgeCount = edges.count()
    
    val loadingTime = (System.currentTimeMillis() - startTimeLoading) / 1000.0
    println(s"Graph loaded in $loadingTime seconds")
    println(s"Number of vertices: $vertexCount")
    println(s"Number of edges: $edgeCount")

    // ─── Run multiple pivot clustering iterations ────────────────────────────────
    println("Starting clustering process...")
    val startTimeClustering = System.currentTimeMillis()
    
    // Number of runs to try
    val runs = 10
    println(s"Performing $runs runs of pivot clustering")
    
    // Generate different random seeds
    val seeds = (1 to runs).map(_ => Random.nextLong())
    
    // Run pivot clustering with each seed and track the best result
    var bestClustering: RDD[(VertexId, Long)] = null
    var bestDisagreements = Long.MaxValue
    
    for ((seed, i) <- seeds.zipWithIndex) {
      println(s"Run ${i+1}/$runs with seed $seed")
      val runStartTime = System.currentTimeMillis()
      
      // Set the random seed
      Random.setSeed(seed)
      
      // Run the pivot clustering algorithm
      val clustering = runPivotClustering(graph)
      
      // Count disagreements with the correct formula
      val disagreements = countDisagreements(graph, clustering)
      
      val runTime = (System.currentTimeMillis() - runStartTime) / 1000.0
      println(s"Run ${i+1} completed in $runTime seconds with $disagreements disagreements")
      
      // Keep track of the best clustering
      if (disagreements < bestDisagreements) {
        bestDisagreements = disagreements
        bestClustering = clustering
        println(s"New best clustering found with $bestDisagreements disagreements")
      }
    }
    
    println(s"Best pivot clustering has $bestDisagreements disagreements")
    
    // ─── Apply local move refinement ────────────────────────────────────────────
    println("Applying local move refinement...")
    val startTimeRefinement = System.currentTimeMillis()
    
    val sweeps = 3
    val finalClustering = localMoveRefinement(graph, bestClustering, sweeps)
    val finalDisagreements = countDisagreements(graph, finalClustering)
    
    val refinementTime = (System.currentTimeMillis() - startTimeRefinement) / 1000.0
    println(s"Local move refinement completed in $refinementTime seconds")
    println(s"Final result: $finalDisagreements disagreements")
    
    val clusteringTime = (System.currentTimeMillis() - startTimeClustering) / 1000.0
    println(s"Total clustering time: $clusteringTime seconds")
    
    // ─── Format results for output ────────────────────────────────────────────
    val result = finalClustering.map{ case (id, cluster) => s"$id,$cluster" }

    // ─── Save as a single CSV file ─────────────────────────────────────────────
    println(s"Saving results to: $outPath")
    val startTimeSaving = System.currentTimeMillis()
    
    finalClustering
      .map { case (v, cid) => s"$v,$cid" }
      .sortBy(identity)  // deterministic order
      .coalesce(1)       // ensure single output file
      .saveAsTextFile(outPath)
    
    val savingTime = (System.currentTimeMillis() - startTimeSaving) / 1000.0
    println(s"Results saved in $savingTime seconds")
    
    val totalTime = (System.currentTimeMillis() - startTimeTotal) / 1000.0
    println(s"Total execution time: $totalTime seconds")
    
    spark.stop()
  }
  
  /**
   * Run the pivot clustering algorithm using Pregel
   */
  def runPivotClustering(graph: Graph[Int, Int]): RDD[(VertexId, Long)] = {
    // Initialize vertices with random keys
    val rnd = new Random()
    val initialGraph: Graph[VData, Int] = graph.mapVertices { case (id, _) =>
      VData(cluster = -1L, key = rnd.nextDouble(), active = true)
    }
    
    // Run Pregel for iterative pivoting
    val pivoted = initialGraph.pregel(
      initialMsg = (-1L, Double.PositiveInfinity),
      maxIterations = 30,
      activeDirection = EdgeDirection.Either
    )(
      // Vertex program
      (id, attr, msg) => {
        var (cluster, key, active) = (attr.cluster, attr.key, attr.active)
        val (bestPivot, bestKey) = msg
        
        if (cluster == -1L) {                   // still unclustered
          if (bestPivot == id) {                // I turned out to be a pivot
            cluster = id
            active = false
          } else if (bestPivot != -1L) {        // somebody claimed me
            cluster = bestPivot
            active = false
          }
        }
        VData(cluster, key, active)
      },
      
      // Send messages (only between still-active vertices)
      triplet => {
        val buf = scala.collection.mutable.ArrayBuffer.empty[(VertexId, Msg)]
        val VData(srcCl, srcKey, srcAct) = triplet.srcAttr
        val VData(dstCl, dstKey, dstAct) = triplet.dstAttr
        
        if (srcAct && dstAct) {
          if (srcKey < dstKey) buf += ((triplet.dstId, (triplet.srcId, srcKey)))
          else if (dstKey < srcKey) buf += ((triplet.srcId, (triplet.dstId, dstKey)))
        }
        buf.iterator
      },
      
      // Merge messages - keep the pivot with the smallest key
      (m1, m2) => if (m1._2 < m2._2) m1 else m2
    )
    
    // Extract final clustering
    pivoted.vertices.map { case (id, data) =>
      val cid = if (data.cluster == -1L) id else data.cluster
      (id, cid)
    }
  }
  
  /**
   * Count disagreements with the correct formula:
   * disagreements = pos-across + neg-inside
   */
  def countDisagreements(graph: Graph[Int, Int], clustering: RDD[(VertexId, Long)]): Long = {
    // Create a broadcast variable for the clustering
    val clusteringMap = clustering.collectAsMap()
    val clusteringBC = graph.vertices.sparkContext.broadcast(clusteringMap)
    
    // Count positive edges across clusters (pos-across)
    val posAcross = graph.triplets.filter { triplet =>
      val srcCluster = clusteringBC.value.getOrElse(triplet.srcId, triplet.srcId)
      val dstCluster = clusteringBC.value.getOrElse(triplet.dstId, triplet.dstId)
      srcCluster != dstCluster
    }.count()
    
    // Count vertices in each cluster
    val sizePerCluster = clustering.map { case (_, cluster) => (cluster, 1L) }
      .reduceByKey(_ + _)
      .collectAsMap()
    
    // Count positive edges within each cluster
    val posInsidePerCluster = graph.triplets
      .filter { triplet =>
        val srcCluster = clusteringBC.value.getOrElse(triplet.srcId, triplet.srcId)
        val dstCluster = clusteringBC.value.getOrElse(triplet.dstId, triplet.dstId)
        srcCluster == dstCluster
      }
      .map { triplet =>
        val cluster = clusteringBC.value.getOrElse(triplet.srcId, triplet.srcId)
        (cluster, 1L)
      }
      .reduceByKey(_ + _)
      .collectAsMap()
    
    // Calculate negative edges within clusters (neg-inside)
    val negInside = sizePerCluster.map { case (cluster, size) =>
      val posInside = posInsidePerCluster.getOrElse(cluster, 0L)
      val totalPossibleEdges = size * (size - 1) / 2
      totalPossibleEdges - posInside
    }.sum
    
    // Total disagreements = pos-across + neg-inside
    posAcross + negInside
  }
  
  /** greedy local-move refinement – never increases the objective            */
  def localMoveRefinement(
        g: Graph[Int,Int],
        clustering: RDD[(VertexId,Long)],
        maxSweeps: Int): RDD[(VertexId,Long)] = {

    var current  = clustering.persist()
    var bestObj  = countDisagreements(g,current)
    println(s"  start objective = $bestObj")

    var sweep = 1
    var improved = true
    while (sweep <= maxSweeps && improved) {

      /* helpers ------------------------------------------------------------ */
      val cidOf = clustering.sparkContext.broadcast(current.collectAsMap())
      val sizeOf = clustering.sparkContext.broadcast(
                    current.map{case (_,c)=>(c,1L)}.reduceByKey(_+_).collectAsMap())

      /* positive edges from v to neighbour clusters ----------------------- */
      val posTo = g.triplets.flatMap{t =>
        val (cS,cD) = (cidOf.value(t.srcId), cidOf.value(t.dstId))
        Iterator(((t.srcId,cD),1L), ((t.dstId,cS),1L))
      }.reduceByKey(_+_)
       .map{case((v,c),cnt)=>(v,(c,cnt))}.groupByKey()

      /* best destination for every vertex --------------------------------- */
      val moves = posTo.rightOuterJoin(current).mapValues{
        case (Some(it),curC) =>
          val nb = it.toMap.withDefaultValue(0L)
          val pCur=nb(curC); val sCur=sizeOf.value(curC)
          var bestΔ=0L; var bestC=curC
          (nb.keysIterator ++ Iterator(curC)).foreach{ trg =>
            if (trg!=curC){
              val Δ = 2L*pCur-2L*nb(trg) + sizeOf.value(trg)-sCur
              if (Δ<bestΔ){bestΔ=Δ; bestC=trg.toLong}
            }}
          bestC
        case (None,curC) => curC
      }.persist()

      /* apply moves -------------------------------------------------------- */
      val candidate = current.leftOuterJoin(moves).mapValues{
        case (curC,Some(newC)) => newC
        case (curC,None)       => curC
      }.persist()

      val candObj = countDisagreements(g,candidate)
      println(s"  sweep $sweep objective = $candObj")

      if (candObj < bestObj) {            // accept sweep
        current.unpersist()
        current = candidate
        bestObj  = candObj
        sweep   += 1
      } else {                            // reject sweep and stop
        candidate.unpersist()
        improved = false
      }
    }
    current
  }
}
