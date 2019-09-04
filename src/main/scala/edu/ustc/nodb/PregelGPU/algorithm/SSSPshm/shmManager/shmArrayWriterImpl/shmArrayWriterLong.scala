package edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayWriterImpl

import edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayWriter

class shmArrayWriterLong(preAllocateSize: Int, fullName: String)
  extends shmArrayWriter(preAllocateSize) {

  def this(pid: Int, preAllocateSize: Int, identifier: String) = {
    this(preAllocateSize, pid + "Long" + identifier)
  }

  var shmName : String = fullName

  type T = Long

  var dataTypeSize : Int = 8

  buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)

  def shmArrayWriterSet(data: Long): Unit = {
    buffer.putLong(data)

  }

}
