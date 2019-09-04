package edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmNamePackager

import java.nio.file.{Files, Paths}

import edu.ustc.nodb.PregelGPU.algorithm.SSSPshm.shmManager.shmPackager

class shmWriterPackager(maxSize: Int) extends shmPackager(maxSize) {

  def addName(shmName: String, counter: Int): Boolean = {

    val pathExist = Files.exists(Paths.get("/dev/shm/" + shmName))

    if (! pathExist) false
    else if (isFull) false
    else {

      shmNameArr(pointer) = shmName
      shmSizeArr(pointer) = counter

      pointer = pointer + 1
      if(pointer == maxSize) isFull = true

      true

    }
  }

  def getNameByUnder(underScore: Int): String = {

    if (underScore >= maxSize) None

    shmNameArr(underScore)

  }

  def getSizeByUnder(underScore: Int): Int = {

    if (underScore >= maxSize) None

    shmSizeArr(underScore)

  }

  def getCapacity : Int = pointer
}
