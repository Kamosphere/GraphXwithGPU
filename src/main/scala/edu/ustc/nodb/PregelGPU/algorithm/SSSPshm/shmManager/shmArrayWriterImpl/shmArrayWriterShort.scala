package edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayWriterImpl

import edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayWriter

class shmArrayWriterShort(preAllocateSize: Int, fullName: String)
  extends shmArrayWriter(preAllocateSize){

  def this(pid: Int, preAllocateSize: Int, identifier: String) = {
    this(preAllocateSize, pid + "Short" + identifier)
  }

  var shmName : String = fullName

  type T = Short

  var dataTypeSize = 2

  buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)

  def shmArrayWriterSet(data: Short): Unit = {
    buffer.putShort(data)

  }

}