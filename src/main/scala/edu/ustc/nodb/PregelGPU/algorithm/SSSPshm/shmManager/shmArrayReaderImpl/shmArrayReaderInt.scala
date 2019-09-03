package edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayReaderImpl

import edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayReader

class shmArrayReaderInt(preAllocateSize: Int, fullName: String)
  extends shmArrayReader(preAllocateSize){

  def this(pid: Int, preAllocateSize: Int, identifier: String) = {
    this(preAllocateSize, pid + "Int" + identifier)
  }

  var shmName : String = fullName

  type T = Int

  var dataTypeSize : Int = 4

  buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)

  def shmArrayReaderGet(): Array[Int] = {
    val t = buffer.asIntBuffer()
    val arrayTest = new Array[Int](t.remaining())
    t.get(arrayTest)
    arrayTest

  }

}