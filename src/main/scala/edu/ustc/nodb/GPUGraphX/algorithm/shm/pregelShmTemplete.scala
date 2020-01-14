package edu.ustc.nodb.GPUGraphX.algorithm.shm

import org.apache.spark.graphx.VertexId

trait pregelShmTemplete [VD, ED, A] extends Serializable{

  def lambda_initMessage: A

  def lambda_globalVertexMergeFunc
  (v1: VertexId, v2: VD, v3: A) : VD

  def lambda_globalReduceFunc
  (v1: A, v2: A): A

}
