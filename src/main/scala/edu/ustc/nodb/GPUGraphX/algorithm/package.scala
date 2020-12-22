package edu.ustc.nodb.GPUGraphX

import org.apache.spark.graphx.VertexId

import scala.collection.mutable

package object algorithm {

  // Define the vertex attribute in GPU-based SSSP project
  // mutable.LinkedHashMap stored the pairs of nearest distance from landmark in order
  type SPMap = Map[VertexId, Double]

  type LPAPair = Map[VertexId, Long]

  type PRPair = (Double, Double)

  // For executing script type, 0 for CPU, other for GPU
  val runningScriptType = 0

  // 0 for running in server with standalone, other for local
  val controller : Int = envControl.controller

  // For opening time consuming record into log4j
  var openTimeLog : Boolean = false
}
