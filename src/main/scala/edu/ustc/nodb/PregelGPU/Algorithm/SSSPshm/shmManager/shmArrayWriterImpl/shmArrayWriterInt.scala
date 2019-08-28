package edu.ustc.nodb.PregelGPU.Algorithm.SSSPshm.shmManager.shmArrayWriterImpl

import edu.ustc.nodb.PregelGPU.Algorithm.SSSPshm.shmManager.shmArrayWriter

class shmArrayWriterInt(preAllocateSize: Int, fullName: String)
  extends shmArrayWriter(preAllocateSize){

  def this(pid: Int, preAllocateSize: Int, identifier: String) = {
    this(preAllocateSize, pid + "Int" + identifier)
  }

  var shmName : String = fullName

  type T = Int

  var dataTypeSize : Int = 4

  buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)

  def shmArrayWriterSet(data: Int): Unit = {
    buffer.putInt(data)

  }
}
