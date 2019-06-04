package edu.ustc.nodb.PregelGPU.Plugin.partitionStrategy

import org.apache.spark.HashPartitioner
import org.apache.spark.graphx._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

class EdgePartitionPreSearch[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED], landMarks: ArrayBuffer[VertexId]) extends Serializable {

  val landMarkVertexIndexed : collection.Map[VertexId, Map[VertexId, Int]] = NeighborCollect.run(g, 2, landMarks)

  val landMarkActiveIndexed : mutable.HashMap[VertexId, ArrayBuffer[VertexId]] = init_landMap()

  val numParts : Int = g.vertices.getNumPartitions

  val landMarkPartitionID : mutable.HashMap[VertexId, PartitionID] = init_landDist()

  val mixingPrime: VertexId = 1125899906842597L

  def init_landMap(): mutable.HashMap[VertexId, ArrayBuffer[VertexId]] ={
    val landMarkMap = new mutable.HashMap[VertexId, ArrayBuffer[VertexId]]

    for(unit <- landMarkVertexIndexed){
      for(kvPair <- unit._2){
        val c = landMarkMap.getOrElse(kvPair._1, new ArrayBuffer[VertexId])
        c.+=(unit._1)
        landMarkMap.put(kvPair._1, c)
      }
    }
    landMarkMap
  }

  def init_landDist(): mutable.HashMap[VertexId, PartitionID] = {
    val landMarkDist = new mutable.HashMap[VertexId, PartitionID]
    var i = 0

    for(unit <- landMarks){
      if(landMarkVertexIndexed.contains(unit) && ! landMarkDist.contains(unit)){
        val landMarkMerge = landMarkVertexIndexed(unit).keySet.toArray
        val pid = (unit % numParts).toInt
        landMarkDist.put(unit, pid)
        for(elem <- landMarkMerge){
          landMarkDist.put(elem, pid)
        }
      }
      else if (! landMarkVertexIndexed.contains(unit) && ! landMarkDist.contains(unit)){
        val pid = (unit % numParts).toInt
        landMarkDist.put(unit, pid)
      }
    }

    landMarkDist
  }


  private def VertexIdPartition(e : EdgeTriplet[VD, ED]): PartitionID = {

    if(landMarks.contains(e.srcId)){
      landMarkPartitionID(e.srcId)
    }

    else if(landMarks.contains(e.dstId)){
      landMarkPartitionID(e.dstId)
    }

    else{

      val edgeSrcMap = landMarkVertexIndexed.getOrElse(e.srcId, Map())
      val edgeDstMap = landMarkVertexIndexed.getOrElse(e.dstId, Map())

      if(edgeSrcMap.nonEmpty && edgeDstMap.isEmpty){
        val random = new Random(System.currentTimeMillis())
        val randomIndex = random.nextInt(edgeSrcMap.keySet.size)
        val resultArr = edgeSrcMap.keySet.toArray
        val result = resultArr(randomIndex)
        landMarkPartitionID(result)
      }

      else if(edgeSrcMap.isEmpty && edgeDstMap.nonEmpty){
        val random = new Random(System.currentTimeMillis())
        val randomIndex = random.nextInt(edgeDstMap.keySet.size)
        val resultArr = edgeDstMap.keySet.toArray
        val result = resultArr(randomIndex)
        landMarkPartitionID(result)
      }

      else {
        // may have memory problem
        val sameCenter = edgeSrcMap.keySet.intersect(edgeDstMap.keySet).toArray

        if(sameCenter.isEmpty){
          // default src centered
          (math.abs(e.dstId * mixingPrime) % numParts).toInt
        }
        else{
          if(sameCenter.length == 1){
            landMarkPartitionID(sameCenter(0))
          }
          else{
            val random = new Random(System.currentTimeMillis())
            val randomIndex = random.nextInt(sameCenter.length)
            val result = sameCenter(randomIndex)
            landMarkPartitionID(result)
          }
        }
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