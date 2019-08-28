package edu.ustc.nodb.PregelGPU.Algorithm.SSSPshm.shmManager.shmArrayWriterImpl

import edu.ustc.nodb.PregelGPU.Algorithm.SSSPshm.shmManager.shmArrayWriter

class shmArrayWriterDouble(preAllocateSize: Int, fullName: String)
  extends shmArrayWriter(preAllocateSize){

  def this(pid: Int, preAllocateSize: Int, identifier: String) = {
    this(preAllocateSize, pid + "Double" + identifier)
  }

  var shmName : String = fullName

  type T = Double

  var dataTypeSize : Int = 8

  buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)

  def shmArrayWriterSet(data: Double): Unit = {
    buffer.putDouble(data)

  }

}