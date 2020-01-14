package edu.ustc.nodb.GPUGraphX.example.SSSP

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{EdgeTriplet, Graph, Pregel, VertexId}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class PregelSparkSSSP (graph: Graph[VertexId, Double],
                       allSource: Broadcast[ArrayBuffer[VertexId]]) extends Serializable {

  // Define the vertex type in standard Pregel in Spark
  // map stored the pairs of nearest distance from landmark
  type SPMap = Map[VertexId, Double]

  /* the standard Pregel in spark to implement the SSSP */

  private def makeMap(x: (VertexId, Double)*) = Map(x: _*)

  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap = {
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Double.MaxValue), spmap2.getOrElse(k, Double.MaxValue))
    }(collection.breakOut) // more efficient alternative to [[collection.Traversable.toMap]]
  }

  val initialMessage : Map[VertexId, Double] = makeMap()

  def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap):
  SPMap = {

    addMaps(attr, msg)

  }

  // Modified to adjust the weighted graph
  // allSource stands for the list
  // broadcast the source list to every partition,
  // which contains the sources of shortest path
  def sendMessage(edge: EdgeTriplet[SPMap, Double]):
  Iterator[(VertexId, SPMap)] = {

    var result = new ArrayBuffer[(VertexId, SPMap)]()
    for (element <- allSource.value) {
      val oldAttr = edge.srcAttr.getOrElse(element, Double.PositiveInfinity)
      val newAttr = edge.dstAttr.getOrElse(element, Double.PositiveInfinity)
      if (oldAttr < newAttr-edge.attr) {
        result.+=((edge.dstId, makeMap(element -> (oldAttr + edge.attr))))
      }
    }
    result.iterator
  }

  def run(): Graph[SPMap, Double] = {

    val spGraph = graph.mapVertices { (vid, attr) =>
      if (allSource.value.contains(vid)) makeMap(vid -> 0) else makeMap()
    }.cache()
    Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMaps)

  }
}
