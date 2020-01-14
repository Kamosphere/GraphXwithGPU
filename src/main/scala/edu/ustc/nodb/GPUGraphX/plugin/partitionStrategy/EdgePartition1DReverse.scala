package edu.ustc.nodb.GPUGraphX.plugin.partitionStrategy

import org.apache.spark.graphx.{PartitionID, PartitionStrategy, VertexId}

case object EdgePartition1DReverse extends PartitionStrategy {

  override def getPartition(src: VertexId,
                            dst: VertexId,
                            numParts: PartitionID):
  PartitionID = {
    val mixingPrime: VertexId = 1125899906842597L
    (math.abs(dst * mixingPrime) % numParts).toInt
  }

  // might be used in label Propagation
}
