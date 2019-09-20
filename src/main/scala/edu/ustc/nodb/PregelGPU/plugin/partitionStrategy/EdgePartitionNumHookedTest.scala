package edu.ustc.nodb.PregelGPU.plugin.partitionStrategy

import edu.ustc.nodb.PregelGPU.envControl
import org.apache.spark.graphx.{PartitionID, PartitionStrategy, VertexId}

case object EdgePartitionNumHookedTest extends PartitionStrategy {

  override def getPartition(src: VertexId,
                            dst: VertexId,
                            numParts: PartitionID):
  PartitionID = {
    val mixingPrime: VertexId = 1125899906842597L
    (math.abs(src / envControl.skippingPartSize * mixingPrime) % numParts).toInt
  }

  // might be used in label Propagation
}
