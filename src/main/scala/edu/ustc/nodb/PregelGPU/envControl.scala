package edu.ustc.nodb.PregelGPU

object envControl {
  // 0 for running in server with standalone, other for local
  val controller : Int = 1

  // true open skipping, false for close skipping
  val runningInSkip : Boolean = false

  // For repartitioning the graph that construct from several sub graphs
  // Test for best situation of skipping
  // WARNING: out of scope variable, modified in test but used in repartition
  var skippingPartSize : Int = 1

  // For opening time consuming record into log4j
  var openTimeLog : Boolean = false

  // For global test of all algorithms
  val allTestGraphVertices = 100000

  // For dataset type
  // 0 for random graph
  // 1 for WRN
  // 2 for orkut
  // 3 for Livejournal
  // 4 for wiki-topcats
  val datasetType = 1

  // For executing script type, 0 for CPU, other for GPU
  val runningScriptType = 1
}
