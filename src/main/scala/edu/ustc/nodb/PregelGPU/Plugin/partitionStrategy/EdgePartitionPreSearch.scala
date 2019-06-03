package edu.ustc.nodb.PregelGPU.Plugin.partitionStrategy

import org.apache.spark.HashPartitioner
import org.apache.spark.graphx._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

class EdgePartitionPreSearch[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED], landMarks: ArrayBuffer[VertexId]) extends Serializable {

  val iterStep : collection.Map[VertexId, Map[VertexId, Int]] = NeighborCollect.run(g, 3, landMarks)

  val landMarkMap = new mutable.HashMap[VertexId, ArrayBuffer[VertexId]]

  for(unit <- iterStep){
    for(kvPair <- unit._2){
      val c = landMarkMap.getOrElse(kvPair._1, new ArrayBuffer[VertexId])
      c.+=(unit._1)
      landMarkMap.put(kvPair._1, c)
    }
  }
  val numParts : Int = g.vertices.getNumPartitions

  private def VertexIdPartition(e : EdgeTriplet[VD, ED]): PartitionID = {
    val mixingPrime: VertexId = 1125899906842597L
    val edgeSrcMap = iterStep.getOrElse(e.srcId, Map())
    val edgeDstMap = iterStep.getOrElse(e.dstId, Map())

    // may have memory problem
    val sameCenter = edgeSrcMap.keySet.intersect(edgeDstMap.keySet).toArray

    if(sameCenter.isEmpty){
      // default src centered
      (math.abs(e.dstId * mixingPrime) % numParts).toInt
    }
    else{
      if(sameCenter.length == 1){
        (math.abs(sameCenter(0) * mixingPrime) % numParts).toInt
      }
      else{
        val random = new Random(System.currentTimeMillis())
        val randomIndex = random.nextInt(sameCenter.length)
        val result = sameCenter(randomIndex)
        (math.abs(result * mixingPrime) % numParts).toInt
      }
    }

  }
  def generateMappedGraph(): Graph[VD, ED] = {
    val partitionedEdges = g.triplets.map(e => (VertexIdPartition(e), e))
      .partitionBy(new HashPartitioner(numParts))
      .map(pair => Edge(pair._2.srcId, pair._2.dstId, pair._2.attr))

    Graph(g.vertices, partitionedEdges)
  }

}