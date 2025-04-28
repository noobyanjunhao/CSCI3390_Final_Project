package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel

object clustering_verifier{
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("verifier")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    sc.setLogLevel("ERROR")

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



    case class VertexData(data1: Long, data2: Long)
      
    val graph_edges = sc.textFile(args(0)).map(line_to_canonical_edge)

    val graph_vertices = sc.textFile(args(1)).map(line => line.split(",")).map( parts => (parts(0).toLong, parts(1).toLong) )

    if(graph_vertices.keys.distinct.count != graph_vertices.count){
      println("A vertex ID showed up more than once in the solution file.")
      sys.exit(1)      
    } 

    val original_vertices = Graph.fromEdges[Int, Int](graph_edges, 0).vertices.keys
    if(!(original_vertices.subtract(graph_vertices.keys).isEmpty() && graph_vertices.keys.subtract(original_vertices).isEmpty() )){
      println("The set of vertices in the solution file does not match that of the inpout file.")
      sys.exit(1)      
    }

    val graph_vertices2 = graph_vertices.map(x => (x._2,x._1))
    val graph_vertices3 = graph_vertices2.join(graph_vertices.map(x => (x._2, 1L)).reduceByKey(_+_)).map({case (a,x) => (x._1,(a,x._2))})

    val graph2: Graph[Tuple2[Long, Long], Int]  = Graph(graph_vertices3, graph_edges, (0L,0L))


    val degrees = graph2.degrees
    val graph3 = graph2.outerJoinVertices(degrees){
      case (id, (a,b), degOpt) => degOpt match {
        case Some(deg) => (a, b ,deg)
        case None => (a,b,0)
      }
    }
    val graph4 = graph3.aggregateMessages[Int](e=>{
        if(e.srcAttr._1 == e.dstAttr._1){
          e.sendToDst(1)
          e.sendToSrc(1)
        }
      }, _+_)

    val graph5 = graph3.outerJoinVertices(graph4){
        (id, x, newOpt) => newOpt match {
        case Some(newAttr) => (x._1,x._2,x._3,newAttr)
        case None => (x._1,x._2,x._3,0)
    }  }
    val graph6 = graph5.mapVertices{ (ID,ver) => 
      {
        val component_size = ver._2
        val degree = ver._3
        val same_component_agreement = ver._4
        (component_size - 1 - same_component_agreement) + (degree - same_component_agreement)
      }
    }
//    graph5.vertices.collect().foreach(println)
    val ans = graph6.vertices.aggregate(0L)({case (x, v) => x + v._2 }, (x,y) => x+y) 
    println("The clustering has a disagreement of: "+(ans/2))
  }
}
