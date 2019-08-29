package edu.ustc.nodb.PregelGPU.Algorithm.SSSP

import java.util

import edu.ustc.nodb.PregelGPU.Algorithm.SPMapWithActive
import edu.ustc.nodb.PregelGPU.envControl
import org.apache.spark.graphx.VertexId

import scala.collection.mutable.ArrayBuffer
import scala.sys.process.Process

class GPUController(vertexSum: Long,
                    edgeSize: Int,
                    sourceList: ArrayBuffer[VertexId],
                    pid :Int)

extends Serializable{

  def this(pid: Int) = this(0, 0, new ArrayBuffer[VertexId], pid)

  val native = new GPUNative
  val sourceSize: Int = sourceList.length

  val resultID : Array[Long] = new Array[Long](vertexSum.toInt)
  val resultAttr : Array[Double] = new Array[Double](vertexSum.toInt * sourceSize)

  var tempVertexSet : VertexSet = _

  System.loadLibrary("PregelGPU")

  // before executing, run the server first
  def GPUEnvEdgeInit(filteredVertex: Array[Long],
                     EdgeSrc: Array[VertexId],
                     EdgeDst: Array[VertexId],
                     EdgeAttr: Array[Double]):
  Unit = {

    GPUShutdown()
    var runningScript = ""

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

    //initialize the source id array
    val sourceId = new util.ArrayList[Long](sourceList.length+(sourceList.length>>1))
    for(unit <- sourceList){
      sourceId.add(unit)
    }

    // if not success, it will run
    var result = false

    while(! result){
      result = native.nativeEnvEdgeInit(filteredVertex, vertexSum, EdgeSrc, EdgeDst, EdgeAttr, sourceId, pid)
    }
  }

  // execute SSSP algorithm
  def GPUMsgExecute(VertexID: Array[Long],
                    VertexActive: Array[Boolean],
                    VertexAttr: Array[Double],
                    vertexCount: Int):
  (ArrayBuffer[(VertexId, SPMapWithActive)], Boolean) = {

    // pass vertices through JNI and get result array back
    var underIndex = native.nativeStepMsgExecute(vertexSum,
      VertexID, VertexActive, VertexAttr,
      vertexCount, edgeSize, sourceSize, pid,
      resultID, resultAttr)

    val needCombine = if(underIndex <= 0) false else true
    underIndex = math.abs(underIndex)

    val startNew = System.nanoTime()

    val results = vertexMsgPackage(underIndex, activeness = true)

    val endNew = System.nanoTime()
    println("Constructing returned arrayBuffer time: " + (endNew - startNew))
    (results, needCombine)

  }

  // execute algorithm while prev iter skipped
  def GPUIterSkipCollect(vertexCount: Int):
  (ArrayBuffer[(VertexId, SPMapWithActive)], Boolean) = {

    //pass vertices through JNI and get arrayBuffer back
    var underIndex = native.nativeSkipStep(vertexSum,
      vertexCount, edgeSize, sourceSize, pid,
      resultID, resultAttr)

    val needCombine = if(underIndex <= 0) false else true
    underIndex = math.abs(underIndex)

    val startNew = System.nanoTime()

    val results = vertexMsgPackage(underIndex, activeness = true)

    val endNew = System.nanoTime()
    println("Constructing returned skipped arrayBuffer time: " + (endNew - startNew))
    (results, needCombine)

  }

  // execute algorithm in final step
  def GPUFinalCollect(vertexCount: Int):
  ArrayBuffer[(VertexId, SPMapWithActive)] = {

    //pass vertices through JNI and get arrayBuffer back
    val underIndex = native.nativeStepFinal(vertexSum,
      vertexCount, edgeSize, sourceSize, pid,
      resultID, resultAttr)

    val startNew = System.nanoTime()

    val results = vertexMsgPackage(underIndex, activeness = false)

    val endNew = System.nanoTime()
    println("Constructing remained arrayBuffer time: " + (endNew - startNew))

    results
  }

  // after executing, close the server and release the shared memory
  def GPUShutdown():Boolean = {

    native.nativeEnvClose(pid)

  }

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