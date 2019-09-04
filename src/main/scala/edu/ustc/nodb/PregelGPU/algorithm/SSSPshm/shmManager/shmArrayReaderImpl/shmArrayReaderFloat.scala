package edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayReaderImpl

import edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmArrayReader

class shmArrayReaderFloat(preAllocateSize: Int, fullName: String)
  extends shmArrayReader(preAllocateSize) {

  def this(pid: Int, preAllocateSize: Int, identifier: String) = {
    this(preAllocateSize, pid + "Float" + identifier)
  }

  var shmName : String = fullName

  type T = Float

  var dataTypeSize : Int = 4

  buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)

  def shmArrayReaderGet(): Array[Float] = {
    val t = buffer.asFloatBuffer()
    val arrayTest = new Array[Float](t.remaining())
    t.get(arrayTest)
    arrayTest

  }

}
