package edu.ustc.nodb.GPUGraphX

object envControl {
  // 0 for running in server with standalone, other for local
  val controller : Int = 1

  // true for open skipping, false for close skipping
  val runningInSkip : Boolean = false

  // For repartitioning the graph that construct from several sub graphs
  // Test for best situation of skipping
  // WARNING: out of scope variable, modified in test but used in repartition
  var skippingPartSize : Int = 1

  // For global test of all algorithms
  val allTestGraphVertices = 4000000

  // For dataset type
  // 0 for random graph
  // 1 for WRN
  // 2 for orkut
  // 3 for Livejournal
  // 4 for wiki-topcats
  // 5 for twitter
  // 6 for uk
  val datasetType = 0

}
