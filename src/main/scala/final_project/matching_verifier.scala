package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel

object matching_verifier{
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("verifier")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
  /* You can either use sc or spark */

    if(args.length != 2) {
      println("Usage: verifier graph_path matching_path")
      sys.exit(1)
    }

    def line_to_canonical_edge(line: String): Edge[Int] = { 
      val x = line.split(",");
      if(x(0).toLong < x(1).toLong)
        Edge(x(0).toLong, x(1).toLong, 1)
      else
        Edge(x(1).toLong, x(0).toLong, 1)
    }



    val graph_edges = sc.textFile(args(0)).map(line_to_canonical_edge)
    val matched_edges = sc.textFile(args(1)).map(line_to_canonical_edge)

    if(matched_edges.distinct.count!= matched_edges.count){
      println("The matched edges contains duplications of an edge.")
      sys.exit(1)
    }

    if(matched_edges.intersection(graph_edges).count != matched_edges.count){
      println("The matched edges are not a subset of the input graph.")
      sys.exit(1)
    }

    val matched_graph = Graph.fromEdges[Int, Int](matched_edges,0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
    if(matched_graph.ops.degrees.aggregate(0)((x,v) => scala.math.max(x, v._2) , (x,y) => scala.math.max(x,y)) >= 2  ){
      println("The matched edges do not form a matching.")
      sys.exit(1)
    }
    println("The matched edges form a matching of size: "+matched_edges.count)
  }
}
