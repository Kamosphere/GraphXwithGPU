package edu.ustc.nodb.GPUGraphX.algorithm.array

import edu.ustc.nodb.GPUGraphX.algorithm.array.SSSP.GPUController
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.util.LongAccumulator

trait packagerTemplete [VD, ED, A] extends Serializable {

  def lambda_edgeImport
  (pid: Int, iter: Iterator[Edge[ED]])
  (iterTimes: Int,
   countOutDegree: collection.Map[VertexId, Int],
   counter: LongAccumulator):
  Unit

  def lambda_GPUExecute
  (pid: Int, idArr: Array[VertexId], activeArr: Array[Boolean], vertexAttr: Array[VD]):
  (Array[VertexId], Array[A], Boolean)

  def lambda_shutDown
  (pid: Int, iter: Iterator[(VertexId, VD)]):
  Unit = {

    val Process = new GPUController(pid)
    var envInit : Boolean = false

    while(! envInit) {
      envInit = Process.GPUShutdown()
    }
  }

}
