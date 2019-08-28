package edu.ustc.nodb.PregelGPU.Algorithm.SSSPshm.shmManager

import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Paths, StandardOpenOption}

abstract class shmArrayReader(readSize: Int)
  extends shmArrayPrototype(readSize: Int) {

  var dataTypeSize : Int

  var shmName : String

  lazy val channel : FileChannel = Files.newByteChannel(Paths.get("/dev/shm/" + shmName),
    StandardOpenOption.READ)
    .asInstanceOf[FileChannel]
  lazy val buffer : MappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, readSize * dataTypeSize)

  def shmReaderClose(): Unit = {

    if(channel.isOpen){
      channel.close()
    }

  }

  def shmArrayReaderGet(): Array[T]

}
