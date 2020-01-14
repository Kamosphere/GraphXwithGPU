package edu.ustc.nodb.GPUGraphX.algorithm.array.SSSP

import java.util

import org.apache.spark.graphx.VertexId

class GPUNative extends Serializable {

  /* the GPU-based method to execute the SSSP algorithm
  possibly modified for template */

  // native function to init the edge
  @native def nativeEnvEdgeInit(filteredVertex: Array[Long],
                                vertexSum: Long,
                                EdgeSrc: Array[VertexId],
                                EdgeDst: Array[VertexId],
                                EdgeAttr: Array[Double],
                                sourceId: util.ArrayList[Long],
                                pid: Int):
  Boolean

  // native function to execute algorithm
  @native def nativeStepMsgExecute(vertexSum: Long,
                                   VertexID: Array[Long],
                                   VertexActive: Array[Boolean],
                                   VertexAttr: Array[Double],
                                   vertexCount: Int,
                                   edgeCount: Int,
                                   markIdSize: Int,
                                   pid: Int,
                                   resultID: Array[Long],
                                   resultAttr: Array[Double]):
  Int

  // native function to execute algorithm
  @native def nativeStepVertexInput(vertexSum: Long,
                                    VertexID: Array[Long],
                                    VertexActive: Array[Boolean],
                                    VertexAttr: Array[Double],
                                    vertexCount: Int,
                                    edgeCount: Int,
                                    markIdSize: Int,
                                    pid: Int):
  Int

  // native function to fetch message in executing
  @native def nativeStepGetMessages(vertexSum: Long,
                                    resultID: Array[Long],
                                    resultAttr: Array[Double],
                                    vertexCount: Int,
                                    edgeCount: Int,
                                    markIdSize: Int,
                                    pid: Int):
  Int

  // native function to fetch merged vertex info after several times executing
  @native def nativeStepGetOldMessages(vertexSum: Long,
                                       resultID: Array[Long],
                                       resultActive: Array[Boolean],
                                       resultTimeStamp: Array[Int],
                                       resultAttr: Array[Double],
                                       vertexCount: Int,
                                       edgeCount: Int,
                                       markIdSize: Int,
                                       pid: Int):
  Int

  // native function to execute algorithm when iter skipped
  @native def nativeSkipVertexInput(vertexSum: Long,
                                    vertexCount: Int,
                                    edgeCount: Int,
                                    markIdSize: Int,
                                    pid: Int,
                                    iterTimes: Int):
  Int

  // native function to close server
  @native def nativeEnvClose(pid: Int):
  Boolean

}
