package edu.ustc.nodb.GPUGraphX.algorithm.shm.CC

import java.util

import org.apache.spark.graphx.util.collection.shmManager.shmNamePackager._

class GPUNativeShm extends Serializable {

  /* the GPU-based method to execute the SSSP algorithm
  possibly modified for template */

  // native function to init the edge
  @native def nativeEnvEdgeInit(filteredVertex: Array[Long],
                                vertexSum: Long,
                                sourceId: util.ArrayList[Long],
                                pid: Int,
                                shmReader: shmReaderPackager):
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

  // native function to close server
  @native def nativeEnvClose(pid: Int):
  Boolean

}
