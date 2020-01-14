package edu.ustc.nodb.GPUGraphX.algorithm.array

import org.apache.spark.graphx.VertexId

trait pregelTemplete [VD, ED, A] extends Serializable{

  def lambda_initGraph
  (v1: VertexId, v2: VertexId): VD

  def lambda_initMessage
  (v1: VertexId): A

  def lambda_globalVertexFunc
  (v1: VertexId, v2: VD, v3: A) : VD

  def lambda_globalReduceFunc
  (v1: A, v2: A): A

}
