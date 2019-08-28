package edu.ustc.nodb.PregelGPU.Algorithm.SSSPshm

import java.util

import edu.ustc.nodb.PregelGPU.Algorithm.SSSPshm.shmManager.shmNamePackager._
import org.apache.spark.graphx.VertexId

private[SSSPshm] class GPUNativeShm extends Serializable {

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

  // native function to execute algorithm
  @native def nativeStepMsgExecute(vertexSum: Long,
                                   shmWriter: shmReaderPackager,
                                   shmReader: shmWriterPackager,
                                   vertexCount: Int,
                                   edgeCount: Int,
                                   markIdSize: Int,
                                   pid: Int):
  Int

  // native function to execute algorithm while prev iter skipped
  @native def nativeSkipStep(vertexSum: Long,
                             vertexCount:Int,
                             edgeSize: Int,
                             markIdSize: Int,
                             pid: Int,
                             shmReader: shmWriterPackager):
  Int

  // native function to execute algorithm for final step
  @native def nativeStepFinal(vertexSum: Long,
                              vertexCount:Int,
                              edgeSize: Int,
                              markIdSize: Int,
                              pid: Int,
                              shmReader: shmWriterPackager):
  Int

  // native function to close server
  @native def nativeEnvClose(pid: Int):
  Boolean


}
