package edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayWriterImpl

import edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayWriter

class shmArrayWriterFloat(preAllocateSize: Int, fullName: String)
  extends shmArrayWriter(preAllocateSize){

  def this(pid: Int, preAllocateSize: Int, identifier: String) = {
    this(preAllocateSize, pid + "Float" + identifier)
  }

  var shmName : String = fullName

  type T = Float

  var dataTypeSize : Int = 4

  buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)

  def shmArrayWriterSet(data: Float): Unit = {
    buffer.putFloat(data)

  }

}