package edu.ustc.nodb.PregelGPU

object envControl {
  // 0 for running in server, other for local standalone
  val controller : Int = 1

  // 0 for open skipping, 1 for close skipping
  val runningInSkip : Boolean = false

  // For repartitioning the graph that construct from several sub graphs
  // Test for best situation of skipping
  // WARNING: out of scope variable, modified in test but used in repartition
  var skippingPartSize : Int = 1
}
