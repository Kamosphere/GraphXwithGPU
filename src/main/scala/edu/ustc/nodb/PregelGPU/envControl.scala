package edu.ustc.nodb.PregelGPU

object envControl {
  // 0 for running in server with standalone, other for local
  val controller : Int = 0

  // true open skipping, false for close skipping
  val runningInSkip : Boolean = false

  // For repartitioning the graph that construct from several sub graphs
  // Test for best situation of skipping
  // WARNING: out of scope variable, modified in test but used in repartition
  var skippingPartSize : Int = 1

  // For opening time consuming record into log4j
  var openTimeLog : Boolean = false

  // For global test of all algorithms
  val allTestGraphVertices = 1000000

  // For executing script type, 0 for CPU, other for GPU
  val runningScriptType = 0
}
