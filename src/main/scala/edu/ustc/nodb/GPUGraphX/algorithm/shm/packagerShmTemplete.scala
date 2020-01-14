package edu.ustc.nodb.GPUGraphX.algorithm.shm

import edu.ustc.nodb.GPUGraphX.algorithm
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.graphx.util.collection.shmManager.shmArrayWriter
import org.apache.spark.graphx.util.collection.shmManager.shmNamePackager.shmWriterPackager
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.util.collection.BitSet

import scala.sys.process.Process

trait packagerShmTemplete [VD, ED, A] extends Serializable {

  def lambda_edgeImport
  (pid: Int, iter: Iterator[Edge[ED]])
  (iterTimes: Int,
   countOutDegree: collection.Map[VertexId, Int],
   counter: LongAccumulator):
  Unit

  def lambda_GPUExecute(pid: Int, writer: shmWriterPackager, modifiedVertexAmount: Int,
                        global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int]):
  (BitSet, Array[A], Boolean)

  def lambda_shmInit(identifier: Array[String], pid: Int):
  (Array[shmArrayWriter], shmWriterPackager)

  def lambda_shmWrite(vid: VertexId, activeness: Boolean, vertexAttr: VD,
                      writer: Array[shmArrayWriter]): Unit

  def lambda_shutDown
  (pid: Int, iter: Iterator[(VertexId, VD)]):
  Unit = {

    var runningScript = ""

    // running script to close server in c++

    // diff in executing environment
    if (algorithm.controller == 0) {
      runningScript = "/usr/local/ssspexample/cpp_native/Graph_Algo/test/"
    }
    else {
      runningScript = "./cpp_native/Graph_Algo/test/"
    }

    runningScript += "ipcrm.sh"

    Process(runningScript).run()

    /*
    val Process = new GPUControllerShm(pid)
    var envInit : Boolean = false

    while(! envInit) {
      envInit = Process.GPUShutdown(1)
    }

     */
  }
}
