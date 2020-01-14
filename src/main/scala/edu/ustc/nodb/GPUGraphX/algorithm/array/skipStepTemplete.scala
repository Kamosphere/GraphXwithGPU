package edu.ustc.nodb.GPUGraphX.algorithm.array

import org.apache.spark.graphx.VertexId

trait skipStepTemplete [VD, ED, A] extends Serializable {

  def lambda_VertexIntoGPU
  (pid: Int, idArr: Array[VertexId], activeArr: Array[Boolean], vertexAttr: Array[VD]):
  (Boolean, Int)

  def lambda_getMessages
  (pid: Int): (Array[VertexId], Array[A])

  def lambda_getOldMessages
  (pid: Int): (Array[VertexId], Array[Boolean], Array[Int], Array[A])

  def lambda_SkipVertexIntoGPU
  (pid: Int, iterTimes: Int): (Boolean, Int)

  def lambda_globalOldMsgReduceFunc
  (v1: (Boolean, Int, A), v2: (Boolean, Int, A)): (Boolean, Int, A)

}
