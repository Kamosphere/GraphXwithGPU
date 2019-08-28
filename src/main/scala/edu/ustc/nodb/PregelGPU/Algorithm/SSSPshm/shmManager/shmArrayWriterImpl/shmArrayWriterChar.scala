package edu.ustc.nodb.PregelGPU.Algorithm.SSSPshm.shmManager.shmArrayWriterImpl

import edu.ustc.nodb.PregelGPU.Algorithm.SSSPshm.shmManager.shmArrayWriter

class shmArrayWriterChar(preAllocateSize: Int, fullName: String)
  extends shmArrayWriter(preAllocateSize){

  def this(pid: Int, preAllocateSize: Int, identifier: String) = {
    this(preAllocateSize, pid + "Char" + identifier)
  }

  var shmName : String = fullName

  type T = Char

  var dataTypeSize : Int = 2

  buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)

  def shmArrayWriterSet(data: Char): Unit = {
    buffer.putChar(data)

  }

}