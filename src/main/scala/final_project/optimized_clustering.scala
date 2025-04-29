package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import scala.util.Random
import scala.collection.mutable.{Map => MutableMap, Set => MutableSet}

object OptimizedClustering {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: optimized_clustering <input_graph_path> <output_clustering_path>")
      sys.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val startTimeTotal = System.currentTimeMillis()
    
    val conf = new SparkConf().setAppName("OptimizedClustering")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println(s"Processing input file: $inputPath")
    val startTimeLoading = System.currentTimeMillis()
    
    // Parse input graph
    val edges = sc.textFile(inputPath)
      .map(line => {
        val parts = line.split(",")
        (parts(0).toLong, parts(1).toLong)
      })
      .distinct()
      .cache()

    // Create a graph representation
    val edgeRDD = edges.map(e => {
      if (e._1 < e._2) Edge(e._1, e._2, 1)
      else Edge(e._2, e._1, 1)
    })

    // Get all vertices
    val vertices = edges.flatMap(e => Seq(e._1, e._2)).distinct().cache()
    
    // Create the graph
    val graph = Graph.fromEdges(edgeRDD, 0).cache()
    
    val loadingTime = (System.currentTimeMillis() - startTimeLoading) / 1000.0
    println(s"Graph loading completed in $loadingTime seconds")
    println(s"Number of vertices: ${vertices.count()}")
    println(s"Number of edges: ${edges.count()}")
    
    // For large graphs, use a partitioned approach
    val startTimeClustering = System.currentTimeMillis()
    val vertexCount = vertices.count()
    
    println(s"Using ${if (vertexCount > 100000) "partitioned" else "local search"} clustering algorithm")
    
    val clustering = if (vertexCount > 100000) {
      partitionedClustering(sc, graph, vertices)
    } else {
      localSearchClustering(sc, graph, vertices)
    }
    
    val clusteringTime = (System.currentTimeMillis() - startTimeClustering) / 1000.0
    println(s"Clustering completed in $clusteringTime seconds")
    
    // Count the number of clusters
    val startTimeStats = System.currentTimeMillis()
    val numClusters = clustering.map(_._2).distinct().count()
    println(s"Number of clusters: $numClusters")
    
    // Save the clustering result
    println(s"Saving results to: $outputPath")
    val startTimeSaving = System.currentTimeMillis()
    clustering.map(v => s"${v._1},${v._2}")
      .saveAsTextFile(outputPath)
    
    val savingTime = (System.currentTimeMillis() - startTimeSaving) / 1000.0
    println(s"Results saved in $savingTime seconds")
    
    val totalTime = (System.currentTimeMillis() - startTimeTotal) / 1000.0
    println(s"Total execution time: $totalTime seconds")
    
    sc.stop()
  }
  
  def partitionedClustering(sc: SparkContext, graph: Graph[Int, Int], vertices: RDD[Long]): RDD[(Long, Long)] = {
    val startTime = System.currentTimeMillis()
    println("Starting partitioned clustering...")
    
    // Partition the graph using connected components for initial clustering
    val startTimeCC = System.currentTimeMillis()
    val connectedComponents = graph.connectedComponents().vertices
    val ccTime = (System.currentTimeMillis() - startTimeCC) / 1000.0
    println(s"Connected components computation completed in $ccTime seconds")
    
    // Refine the clustering using local search within each component
    val startTimePartitioning = System.currentTimeMillis()
    val vertexPartitions = connectedComponents.join(vertices.map(v => (v, 0)))
      .map { case (vertexId, (componentId, _)) => (componentId, vertexId) }
      .groupByKey()
    
    val numComponents = vertexPartitions.count()
    println(s"Graph partitioned into $numComponents connected components")
    val partitioningTime = (System.currentTimeMillis() - startTimePartitioning) / 1000.0
    println(s"Partitioning completed in $partitioningTime seconds")
      
    // Process each component in parallel
    val startTimeRefinement = System.currentTimeMillis()
    val refinedClustering = vertexPartitions.flatMap { case (componentId, verticesInComponent) =>
      // Create a subgraph for this component
      val subgraphVertices = verticesInComponent.toSet
      val subgraphEdges = graph.edges.filter(e => 
        subgraphVertices.contains(e.srcId) && subgraphVertices.contains(e.dstId)
      )
      
      // Apply local clustering to this subgraph
      val localGraph = Graph.fromEdges(subgraphEdges, 0)
      val localVertices = sc.parallelize(verticesInComponent.toSeq)
      
      // Use a greedy algorithm for this component
      greedyClustering(localGraph, localVertices, componentId)
    }
    
    val refinementTime = (System.currentTimeMillis() - startTimeRefinement) / 1000.0
    println(s"Refinement of all components completed in $refinementTime seconds")
    
    val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
    println(s"Partitioned clustering completed in $totalTime seconds")
    
    refinedClustering
  }
  
  def greedyClustering(graph: Graph[Int, Int], vertices: RDD[Long], baseClusterId: Long): Array[(Long, Long)] = {
    val startTime = System.currentTimeMillis()
    
    // Convert to local collections for faster processing
    val localVertices = vertices.collect()
    val localEdges = graph.edges.collect()
    
    // Build adjacency list
    val adjacencyList = MutableMap[Long, MutableSet[Long]]()
    for (v <- localVertices) {
      adjacencyList(v) = MutableSet[Long]()
    }
    
    for (e <- localEdges) {
      adjacencyList(e.srcId).add(e.dstId)
      adjacencyList(e.dstId).add(e.srcId)
    }
    
    // Greedy clustering
    var clusterId = baseClusterId * 1000 // Ensure unique cluster IDs
    val clusterAssignments = MutableMap[Long, Long]()
    val unassigned = MutableSet[Long]() ++= localVertices
    
    while (unassigned.nonEmpty) {
      // Pick a random unassigned vertex
      val vertex = unassigned.head
      clusterId += 1
      
      // Create a new cluster
      val cluster = MutableSet[Long](vertex)
      clusterAssignments(vertex) = clusterId
      unassigned.remove(vertex)
      
      // Find vertices that have more connections to this cluster than outside
      var changed = true
      while (changed) {
        changed = false
        
        for (v <- unassigned.toArray) {
          // Count connections to the current cluster
          val connectionsToCluster = adjacencyList(v).count(cluster.contains)
          val connectionsOutside = adjacencyList(v).size - connectionsToCluster
          
          // If more connections inside than outside, add to cluster
          if (connectionsToCluster > connectionsOutside) {
            cluster.add(v)
            clusterAssignments(v) = clusterId
            unassigned.remove(v)
            changed = true
          }
        }
      }
    }
    
    val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
    println(s"Greedy clustering for component $baseClusterId completed in $totalTime seconds with ${clusterAssignments.size} vertices")
    
    clusterAssignments.toArray
  }
  
  def localSearchClustering(sc: SparkContext, graph: Graph[Int, Int], vertices: RDD[Long]): RDD[(Long, Long)] = {
    val startTime = System.currentTimeMillis()
    println("Starting local search clustering...")
    
    // Initial clustering: each vertex in its own cluster
    var clustering = vertices.map(v => (v, v)).cache()
    
    // Iterative improvement
    val maxIterations = 5
    var iteration = 0
    var improved = true
    
    while (improved && iteration < maxIterations) {
      val iterStartTime = System.currentTimeMillis()
      iteration += 1
      improved = false
      
      // For each vertex, consider moving it to a different cluster
      val improvements = vertices.map { vertex =>
        // Get current cluster assignment
        val currentCluster = clustering.filter(_._1 == vertex).first()._2
        
        // Get neighbors
        val neighbors = graph.edges
          .filter(e => e.srcId == vertex || e.dstId == vertex)
          .map(e => if (e.srcId == vertex) e.dstId else e.srcId)
          .collect()
        
        // Get neighbor clusters
        val neighborClusters = clustering
          .filter(v => neighbors.contains(v._1))
          .map(_._2)
          .distinct()
          .collect()
          .toSet + currentCluster // Include current cluster
        
        // Evaluate each possible cluster assignment
        val clusterScores = neighborClusters.map { candidateCluster =>
          // Count agreements/disagreements if vertex moves to this cluster
          val score = neighbors.map { neighbor =>
            val neighborCluster = clustering.filter(_._1 == neighbor).first()._2
            if ((candidateCluster == neighborCluster) == true) 1 else -1
          }.sum
          
          (candidateCluster, score)
        }
        
        // Find best cluster
        val bestCluster = clusterScores.maxBy(_._2)._1
        
        // Return improvement if any
        if (bestCluster != currentCluster) {
          improved = true
          (vertex, bestCluster)
        } else {
          (vertex, currentCluster)
        }
      }
      
      // Update clustering
      clustering = improvements.cache()
      
      val iterTime = (System.currentTimeMillis() - iterStartTime) / 1000.0
      println(s"Iteration $iteration completed in $iterTime seconds, improved: $improved")
    }
    
    val numClusters = clustering.map(_._2).distinct().count()
    val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
    println(s"Local search clustering completed in $totalTime seconds with $numClusters clusters after $iteration iterations")
    
    clustering
  }
} 