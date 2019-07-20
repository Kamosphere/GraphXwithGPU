package edu.ustc.nodb.PregelGPU.Algorithm.SSSPBatch

import java.util

import org.apache.spark.graphx.VertexId

private[SSSPBatch] class GPUNativeBatch extends Serializable {

  /* the GPU-based method to execute the SSSP algorithm
  possibly modified for template */

  // native function to init the edge
  @native def nativeEnvEdgeInit(filteredVertex: Array[Long],
                                vertexSum: Long,
                                EdgeSrc: Array[VertexId],
                                EdgeDst: Array[VertexId],
                                EdgeAttr: Array[Double],
                                sourceId: util.ArrayList[Long],
                                pid:Int):
  Boolean


  @native def nativeStepMsgInitInput(vertexSum: Long,
                                     VertexID: Array[Long],
                                     VertexActive: Array[Boolean],
                                     VertexAttr: Array[Double],
                                     vertexCount:Int,
                                     edgeCount: Int,
                                     markIdSize: Int,
                                     pid: Int,
                                     resultID: Array[Long],
                                     resultAttr: Array[Double]):
  Int

  // TODO: batch array copy
  @native def nativeStepMsgBatchInput(vertexSum: Long,
                                      VertexID: Array[Long],
                                      VertexActive: Array[Boolean],
                                      VertexAttr: Array[Double],
                                      vertexCount:Int,
                                      edgeCount: Int,
                                      markIdSize: Int,
                                      pid: Int):
  Boolean

  // TODO: trigger to execute
  // native function to execute algorithm
  @native def nativeStepMsgBatchExecute(vertexSum: Long,
                                vertexCount:Int,
                                edgeCount: Int,
                                markIdSize: Int,
                                pid: Int,
                                resultID: Array[Long],
                                resultAttr: Array[Double]):
  Int

  // native function to execute algorithm for final step
  @native def nativeStepFinal(vertexSum: Long,
                              vertexCount:Int,
                              edgeSize: Int,
                              markIdSize: Int,
                              pid: Int,
                              resultID: Array[Long],
                              resultAttr: Array[Double]):
  Int

  // native function to execute algorithm while prev iter skipped
  @native def nativeSkipStep(vertexSum: Long,
                             vertexCount:Int,
                             edgeSize: Int,
                             markIdSize: Int,
                             pid: Int,
                             resultID: Array[Long],
                             resultAttr: Array[Double]):
  Int


  // native function to close server
  @native def nativeEnvClose(pid: Int):
  Boolean


}
