package edu.ustc.nodb.PregelGPU.Algorithm.SSSPshm

import java.nio.file.{Files, Path, Paths}
import java.util

import edu.ustc.nodb.PregelGPU.Algorithm.SPMapWithActive
import edu.ustc.nodb.PregelGPU.Algorithm.SSSPshm.shmManager.shmArrayReaderImpl.{shmArrayReaderDouble, shmArrayReaderLong}
import edu.ustc.nodb.PregelGPU.Algorithm.SSSPshm.shmManager.shmNamePackager.{shmReaderPackager, shmWriterPackager}
import edu.ustc.nodb.PregelGPU.envControl
import org.apache.spark.graphx.VertexId

import scala.collection.mutable.ArrayBuffer
import scala.sys.process.Process

class GPUControllerShm(vertexSum: Long,
                       edgeSize: Int,
                       sourceList: ArrayBuffer[VertexId],
                       pid :Int)

extends Serializable{

  def this(pid: Int) = this(0, 0, new ArrayBuffer[VertexId], pid)

  // native interface
  val native = new GPUNativeShm

  val sourceSize: Int = sourceList.length

  var resultID : Array[Long] = _
  var resultAttr : Array[Double] = _

  var tempVertexSet : VertexSet = _

  System.loadLibrary("PregelGPUShm")

  // before executing, run the server first
  def GPUEnvEdgeInit(filteredVertex: Array[Long],
                     EdgeSrc: Array[VertexId],
                     EdgeDst: Array[VertexId],
                     EdgeAttr: Array[Double]):
  Unit = {

    GPUShutdown(0)
    var runningScript = ""

    // running script to activate server
    if (envControl.controller == 0){
      runningScript =
        "/usr/local/ssspexample/cpp_native/build/bin/srv_UtilServerTest_BellmanFordGPU " + vertexSum.toString +
          " " + EdgeSrc.length.toString + " " + sourceList.length.toString + " " + pid.toString

    }
    else {
      runningScript =
        "./cpp_native/build/bin/srv_UtilServerTest_BellmanFordGPU " + vertexSum.toString +
          " " + EdgeSrc.length.toString + " " + sourceList.length.toString + " " + pid.toString

    }

    Process(runningScript).run()

    // initialize the source id array
    val sourceId = new util.ArrayList[Long](sourceList.length+(sourceList.length>>1))
    for(unit <- sourceList){
      sourceId.add(unit)
    }

    // if native method not success, it will run until finished
    var result = false

    while(! result){
      result = native.nativeEnvEdgeInit(filteredVertex, vertexSum, EdgeSrc, EdgeDst, EdgeAttr, sourceId, pid)
    }
  }

  // execute SSSP algorithm
  def GPUMsgExecute(VertexID: String,
                    VertexActive: String,
                    VertexAttr: String,
                    vertexCount: Int):
  (ArrayBuffer[(VertexId, SPMapWithActive)], Boolean) = {

    // create reader list to get input array in shm
    val shmReader = new shmReaderPackager(3)

    shmReader.addName(VertexID, vertexCount)
    shmReader.addName(VertexActive, vertexCount)
    shmReader.addName(VertexAttr, vertexCount * sourceSize)

    // create writer list to return shm file names
    val shmWriter = new shmWriterPackager(2)

    // pass reader and writer through JNI
    var underIndex = native.nativeStepMsgExecute(vertexSum,
      shmReader, shmWriter,
      vertexCount, edgeSize, sourceSize, pid)

    val needCombine = if(underIndex <= 0) false else true
    underIndex = math.abs(underIndex)

    // read files in writer list ( already written in c++ )
    val resultIDReader = new shmArrayReaderLong(
      shmWriter.getSizeByUnder(0), shmWriter.getNameByUnder(0))
    val resultAttrReader = new shmArrayReaderDouble(
      shmWriter.getSizeByUnder(1), shmWriter.getNameByUnder(1))

    resultID = resultIDReader.shmArrayReaderGet()
    resultAttr = resultAttrReader.shmArrayReaderGet()

    val startNew = System.nanoTime()

    val results = vertexMsgPackage(underIndex, activeness = true)

    val endNew = System.nanoTime()
    println("Constructing returned arrayBuffer time: " + (endNew - startNew))
    (results, needCombine)

  }

  // execute algorithm while prev iter skipped
  def GPUIterSkipCollect(vertexCount: Int):
  (ArrayBuffer[(VertexId, SPMapWithActive)], Boolean) = {

    val shmWriter = new shmWriterPackager(2)

    // pass writer through JNI, for the data in the last iter is in the server
    var underIndex = native.nativeSkipStep(vertexSum,
      vertexCount, edgeSize, sourceSize, pid,
      shmWriter)

    val needCombine = if(underIndex <= 0) false else true
    underIndex = math.abs(underIndex)

    val resultIDReader = new shmArrayReaderLong(
      shmWriter.getSizeByUnder(0), shmWriter.getNameByUnder(0))
    val resultAttrReader = new shmArrayReaderDouble(
      shmWriter.getSizeByUnder(1), shmWriter.getNameByUnder(1))

    resultID = resultIDReader.shmArrayReaderGet()
    resultAttr = resultAttrReader.shmArrayReaderGet()

    val startNew = System.nanoTime()

    val results = vertexMsgPackage(underIndex, activeness = true)

    val endNew = System.nanoTime()
    println("Constructing returned skipped arrayBuffer time: " + (endNew - startNew))
    (results, needCombine)

  }

  // execute algorithm in final step
  def GPUFinalCollect(vertexCount: Int):
  ArrayBuffer[(VertexId, SPMapWithActive)] = {

    val shmWriter = new shmWriterPackager(2)

    // pass writer through JNI, in order to get last skipped data back
    val underIndex = native.nativeStepFinal(vertexSum,
      vertexCount, edgeSize, sourceSize, pid,
      shmWriter)

    val resultIDReader = new shmArrayReaderLong(
      shmWriter.getSizeByUnder(0), shmWriter.getNameByUnder(0))
    val resultAttrReader = new shmArrayReaderDouble(
      shmWriter.getSizeByUnder(1), shmWriter.getNameByUnder(1))

    resultID = resultIDReader.shmArrayReaderGet()
    resultAttr = resultAttrReader.shmArrayReaderGet()

    val startNew = System.nanoTime()

    val results = vertexMsgPackage(underIndex, activeness = false)

    val endNew = System.nanoTime()
    println("Constructing remained arrayBuffer time: " + (endNew - startNew))

    results
  }

  // after executing, close the server and release the shared memory
  def GPUShutdown(runningStep: Int): Boolean = {

    // 0 for the first iter, other for the times of iter
    if(runningStep == 0){

    }
    else{

      // remove all shm files in file way
      val k = Files.newDirectoryStream( Paths.get("/dev/shm/") ).iterator()
      var pathTemp: Path = null
      while(k.hasNext){
          pathTemp = k.next()
          Files.deleteIfExists(pathTemp)

        }
    }
    native.nativeEnvClose(pid)

  }

  // package the returned array into iterable data structure
  def vertexMsgPackage(underIndex: Int, activeness: Boolean):
  ArrayBuffer[(VertexId, SPMapWithActive)] = {

    val results = new ArrayBuffer[(VertexId, SPMapWithActive)]

    for(i <- 0 until underIndex){

      // package the vertex as vertex-like format
      tempVertexSet = new VertexSet(resultID(i), activeness)
      var invalidDetector = 0.0
      for(j <- sourceList.indices){
        invalidDetector = resultAttr(i * sourceSize + j)
        if(invalidDetector < Int.MaxValue) tempVertexSet.addAttr(sourceList(j), resultAttr(i * sourceSize + j))
      }

      results.+=(tempVertexSet.TupleReturn())
    }

    results
  }

}
