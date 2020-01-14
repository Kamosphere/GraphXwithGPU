package edu.ustc.nodb.GPUGraphX.plugin.partitionStrategy

import org.apache.spark.HashPartitioner
import org.apache.spark.graphx._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.control.Breaks._

// It was addressed to partition the graph in a topological way
// search the second jump of neighborhood for landmark
// then part them together
// in order to avoid the sync process in the first several iter
//
// Efficiency needs to be improved
class EdgePartitionPreSearch[VD: ClassTag, ED: ClassTag]
(g: Graph[VD, ED], landMarks: ArrayBuffer[VertexId]) extends Serializable {

  val landMarkVertexIndexed : collection.Map[VertexId, Map[VertexId, Int]]
  = NeighborCollect.run(g, 2, landMarks)

  val landMarkActiveIndexed : mutable.HashMap[VertexId, ArrayBuffer[VertexId]]
  = init_landMap()

  val numParts : Int
  = g.vertices.getNumPartitions

  val landMarkPartitionID : mutable.HashMap[VertexId, PartitionID]
  = init_landDist()

  val mixingPrime: VertexId = 1125899906842597L

  // To generate map centered on landmarks
  def init_landMap(): mutable.HashMap[VertexId, ArrayBuffer[VertexId]] = {
    val landMarkMap = new mutable.HashMap[VertexId, ArrayBuffer[VertexId]]

    for (unit <- landMarkVertexIndexed) {
      for (kvPair <- unit._2) {
        val c = landMarkMap.getOrElse(kvPair._1, new ArrayBuffer[VertexId])
        c.+=(unit._1)
        landMarkMap.put(kvPair._1, c)
      }
    }
    landMarkMap
  }

  // To generate the partition distribution
  def init_landDist(): mutable.HashMap[VertexId, PartitionID] = {

    val landMarkDist = new mutable.HashMap[VertexId, PartitionID]

    val multipleVertices = new ArrayBuffer[ArrayBuffer[VertexId]]
    val uniqueVertices = new ArrayBuffer[ArrayBuffer[VertexId]]

    for (unit <- landMarkVertexIndexed) {
      if (unit._2.size > 1) {
        val insert = ArrayBuffer(unit._2.keySet.toArray: _*)
        multipleVertices.+=(insert)
      }
    }

    // merge landmarks exist in multipleVertices
    breakable {
      while (multipleVertices.nonEmpty) {
        var firstElems = multipleVertices(0)

        for(i <- multipleVertices.indices.reverse) {
          val merged = firstElems.union(multipleVertices(i)).distinct
          if(merged.length != multipleVertices(i).length + firstElems.length) {
            firstElems = merged
            multipleVertices.remove(i)
          }
        }

        uniqueVertices.+=(firstElems)

        // stop while all landmarks are contained
        if(firstElems.length == landMarks.length) break()
      }
    }

    // if no vertices belong to multiple landmarks
    if(uniqueVertices.isEmpty) {
      for (unit <- landMarks) {
        uniqueVertices.+=(ArrayBuffer(unit))
      }
    }

    // apart into map
    var i = 0
    for(unit <- uniqueVertices) {
      for (elem <- unit) {
        landMarkDist.put(elem, i%numParts)
      }
      i = i + 1
    }

    landMarkDist
  }

  private def VertexIdPartition(e : EdgeTriplet[VD, ED]): PartitionID = {

    // val checkNum = 169

    if (landMarks.contains(e.srcId)) {
      landMarkPartitionID(e.srcId)
    }

    else if (landMarks.contains(e.dstId)) {
      landMarkPartitionID(e.dstId)
    }

    else {
      val random = new Random(System.currentTimeMillis())

      val edgeSrcMap = landMarkVertexIndexed.getOrElse(e.srcId, Map())
      val edgeDstMap = landMarkVertexIndexed.getOrElse(e.dstId, Map())

      if (edgeSrcMap.nonEmpty && edgeDstMap.isEmpty) {
        val randomIndex = random.nextInt(edgeSrcMap.keySet.size)
        val resultArr = edgeSrcMap.keySet.toArray
        val result = resultArr(randomIndex)

        landMarkPartitionID(result)
      }

      else if (edgeSrcMap.isEmpty && edgeDstMap.nonEmpty) {
        val randomIndex = random.nextInt(edgeDstMap.keySet.size)
        val resultArr = edgeDstMap.keySet.toArray
        val result = resultArr(randomIndex)

        landMarkPartitionID(result)
      }

      else if (edgeSrcMap.isEmpty && edgeDstMap.isEmpty) {

        (math.abs(e.dstId * mixingPrime) % numParts).toInt
      }

      else {
        // may have memory problem by intersect method
        val sameCenter = edgeSrcMap.keySet.intersect(edgeDstMap.keySet).toArray

        if (sameCenter.isEmpty) {
          val combineCenter = edgeSrcMap.keySet.union(edgeDstMap.keySet).toArray.distinct
          val randomIndex = random.nextInt(combineCenter.length)
          val result = combineCenter(randomIndex)

          landMarkPartitionID(result)

        }
        else {
          if (sameCenter.length == 1) {
            landMarkPartitionID(sameCenter(0))
          }
          else {
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
