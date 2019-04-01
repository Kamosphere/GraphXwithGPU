package edu.ustc.nodb.SSSP

import org.apache.spark.graphx.{PartitionID, PartitionStrategy, VertexId}

// Assigns edges to partitions using only the destination vertex ID,
// colocating edges with the same destination.

case object EdgePartition1DReverse extends PartitionStrategy {
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    val mixingPrime: VertexId = 1125899906842597L
    (math.abs(dst * mixingPrime) % numParts).toInt
  }
}