package edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayWriterImpl

import edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayWriter

class shmArrayWriterByte(preAllocateSize: Int, fullName: String)
  extends shmArrayWriter(preAllocateSize) {

  def this(pid: Int, preAllocateSize: Int, identifier: String) = {
    this(preAllocateSize, pid + "Byte" + identifier)
  }

  var shmName : String = fullName

  type T = Byte

  var dataTypeSize : Int = 1

  buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)

  def shmArrayWriterSet(data: Byte): Unit = {
    buffer.put(data)

  }

}
