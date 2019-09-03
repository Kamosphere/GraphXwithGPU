package edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayWriterImpl

import edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayWriter

class shmArrayWriterBoolean(preAllocateSize: Int, fullName: String)
  extends shmArrayWriter(preAllocateSize){

  def this(pid: Int, preAllocateSize: Int, identifier: String) = {
    this(preAllocateSize, pid + "Boolean" + identifier)
  }

  var shmName : String = fullName

  type T = Boolean

  var dataTypeSize : Int = 1

  buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)

  def shmArrayWriterSet(data: Boolean): Unit = {

    if(data) buffer.put(1.toByte)
    else buffer.put(0.toByte)

  }

}