package edu.ustc.nodb.PregelGPU.algorithm.SSSP

import java.util

import edu.ustc.nodb.PregelGPU.algorithm.SPMap
import edu.ustc.nodb.PregelGPU.envControl

import org.apache.spark.graphx.VertexId

import scala.collection.mutable.ArrayBuffer
import scala.sys.process.Process

class GPUController(vertexSum: Long,
                    vertexCount: Int,
                    edgeCount: Int,
                    sourceList: ArrayBuffer[VertexId],
                    pid: Int) extends Serializable {

  // scalastyle:off println

  // native interface
  val native = new GPUNative

  def this(pid: Int) = this(0, 0, 0, new ArrayBuffer[VertexId], pid)

  val sourceSize: Int = sourceList.length

  val resultID : Array[Long] = new Array[Long](vertexCount)
  val resultAttr : Array[Double] = new Array[Double](vertexCount * sourceSize)

  System.loadLibrary("SSSPGPU")

  def GPUServerActive():
  Unit = {

    var runningScript = ""

    // running script to activate server
    if (envControl.controller == 0) {
      runningScript =
        "/usr/local/ssspexample/cpp_native/build/bin/srv_UtilServerTest_BellmanFordGPU " +
          vertexSum.toString + " " + edgeCount.toString + " " +
          sourceList.length.toString + " " + pid.toString

    }
    else {
      runningScript =
        "./cpp_native/build/bin/srv_UtilServerTest_BellmanFordGPU " +
          vertexSum.toString + " " + edgeCount.toString + " " +
          sourceList.length.toString + " " + pid.toString

    }

    Process(runningScript).run()
  }

  // before executing, run the server first
  def GPUEnvEdgeInit(filteredVertex: Array[Long],
                     EdgeSrc: Array[VertexId],
                     EdgeDst: Array[VertexId],
                     EdgeAttr: Array[Double]):
  Unit = {

    GPUShutdown()

    GPUServerActive()

    // initialize the source id array
    val sourceId = new util.ArrayList[Long](sourceList.length + (sourceList.length >> 1))
    for(unit <- sourceList) {
      sourceId.add(unit)
    }

    // if native method not success, it will run until finished
    var result = false

    while(! result) {
      result = native.nativeEnvEdgeInit(
        filteredVertex, vertexSum, EdgeSrc, EdgeDst, EdgeAttr, sourceId, pid)
    }
  }

  // execute SSSP algorithm
  def GPUMsgExecute(VertexID: Array[Long],
                    VertexActive: Array[Boolean],
                    VertexAttr: Array[Double],
                    vertexCount: Int):
  (Array[VertexId], Array[SPMap], Boolean) = {

    // pass vertices through JNI and get result array back
    var underIndex = native.nativeStepMsgExecute(vertexSum,
      VertexID, VertexActive, VertexAttr,
      vertexCount, edgeCount, sourceSize, pid,
      resultID, resultAttr)

    val needCombine = if (underIndex <= 0) false else true
    underIndex = math.abs(underIndex)

    // val startNew = System.nanoTime()

    val results = vertexAttrPackage(underIndex)

    // val endNew = System.nanoTime()
    // println("Constructing returned arrayBuffer time: " + (endNew - startNew))
    (resultID, results, needCombine)

  }

  // execute algorithm while prev iter skipped
  def GPUIterSkipCollect(vertexCount: Int):
  (Array[VertexId], Array[SPMap], Boolean) = {

    // pass vertices through JNI and get arrayBuffer back
    var underIndex = native.nativeSkipStep(vertexSum,
      vertexCount, edgeCount, sourceSize, pid,
      resultID, resultAttr)

    val needCombine = if (underIndex <= 0) false else true
    underIndex = math.abs(underIndex)

    // val startNew = System.nanoTime()

    val results = vertexAttrPackage(underIndex)

    // val endNew = System.nanoTime()
    // println("Constructing returned skipped array time: " + (endNew - startNew))
    (resultID, results, needCombine)

  }

  // execute algorithm in final step
  def GPUFinalCollect(vertexCount: Int):
  (Array[VertexId], Array[SPMap], Boolean) = {

    // pass vertices through JNI and get array back
    val underIndex = native.nativeStepFinal(vertexSum,
      vertexCount, edgeCount, sourceSize, pid,
      resultID, resultAttr)

    // val startNew = System.nanoTime()

    val results = vertexAttrPackage(underIndex)

    // val endNew = System.nanoTime()
    // println("Constructing remained array time: " + (endNew - startNew))

    (resultID, results, false)
  }

  def vertexAttrPackage(underIndex: Int):
  Array[SPMap] = {

    val results = new Array[SPMap](underIndex)

    var tempVertexAttr : SPMap = null

    for(i <- 0 until underIndex) {

      // package the vertex attr
      tempVertexAttr = makeMap()
      var invalidDetector = 0.0
      for(j <- sourceList.indices) {
        invalidDetector = resultAttr(i * sourceSize + j)
        if(invalidDetector < Double.MaxValue) {
          tempVertexAttr.+=((sourceList(j), resultAttr(i * sourceSize + j)))
        }
      }

      results(i) = tempVertexAttr
    }

    results
  }

  private def makeMap(x: (VertexId, Double)*) = Map(x: _*)

  // after executing, close the server
  def GPUShutdown(): Boolean = {

    native.nativeEnvClose(pid)

  }

  // scalastyle:on println
}
