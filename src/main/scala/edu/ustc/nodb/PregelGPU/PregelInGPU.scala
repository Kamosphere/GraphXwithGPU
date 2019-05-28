package edu.ustc.nodb.PregelGPU

import edu.ustc.nodb.PregelGPU.Algorithm.SSSP.GPUNative
import edu.ustc.nodb.PregelGPU.Algorithm.lambdaTemplete
import edu.ustc.nodb.PregelGPU.Plugin.GraphXModified
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.graphx.PartitionStrategy.EdgePartition1D
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object PregelInGPU{

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VertexId, ED],
                                      maxIterations: Int = Int.MaxValue)
                                     (algorithm: lambdaTemplete[VD, ED])
  : Graph[VD, ED] = {

    // initiate the graph
    val spGraph = graph.mapVertices ((vid, attr) => algorithm.lambda_initGraph(vid, attr)).partitionBy(EdgePartition1D).cache()

    var g = spGraph

    val countOutDegree = g.outDegrees.collectAsMap()

    val sc = org.apache.spark.SparkContext.getOrCreate()

    val ifFilteredCounter = sc.longAccumulator("ifFilteredCounter")

    var beforeCounter = ifFilteredCounter.value

    var iterTimes = 0

    var partitionSplit : collection.Map[Int,(Int, Int)] = g.triplets.mapPartitionsWithIndex((pid, Iter)=>{
      algorithm.lambda_partitionSplit(pid, Iter)
    }).collectAsMap()

    var modifiedSubGraph: RDD[(VertexId, VD)] = g.triplets.mapPartitionsWithIndex((pid, iter) =>
      algorithm.lambda_ModifiedSubGraph_MPBI_afterPartition(pid, iter)(iterTimes, countOutDegree, partitionSplit, ifFilteredCounter))

    // get the vertex number and edge number in every partition
    // in order to avoid allocation arraycopy

    // combine the vertex messages through partitions
    var vertexModified = modifiedSubGraph.reduceByKey((v1, v2) =>
      algorithm.lambda_ReduceByKey(v1, v2)).cache()

    // get the amount of active vertices
    var activeMessages = vertexModified.count()

    var afterCounter = ifFilteredCounter.value

    iterTimes = 1
    var prevG : Graph[VD, ED] = null

    val ifRepartition = false

    //loop
    while(activeMessages > 0 && iterTimes < maxIterations){

      val startTime = System.nanoTime()
      prevG = g
      val oldVertexModified = vertexModified
      ifFilteredCounter.reset()
      val tempBeforeCounter = ifFilteredCounter.sum

      if(ifRepartition){
        g = GraphXModified.joinVerticesDefault(g, vertexModified)((vid, v1, v2) =>
          algorithm.lambda_JoinVerticesDefaultFirst(vid, v1, v2))(vAttr =>
          algorithm.lambda_JoinVerticesDefaultSecond(vAttr))
          .cache()

        partitionSplit = g.triplets.mapPartitionsWithIndex((pid, Iter)=>{
          algorithm.lambda_partitionSplit(pid, Iter)
        }).collectAsMap()

        modifiedSubGraph = g.triplets.mapPartitionsWithIndex((pid, iter) =>
          algorithm.lambda_ModifiedSubGraph_MPBI_afterPartition(pid, iter)(iterTimes, countOutDegree, partitionSplit, ifFilteredCounter))
      }
      else{
        if(afterCounter != beforeCounter){
          //combine the messages with graph
          g = GraphXModified.joinVerticesDefault(g, vertexModified)((vid, v1, v2) =>
            algorithm.lambda_JoinVerticesDefaultFirst(vid, v1, v2))(vAttr =>
            algorithm.lambda_JoinVerticesDefaultSecond(vAttr))
            .cache()

          //run the main process
          modifiedSubGraph = g.triplets.mapPartitionsWithIndex((pid, iter) =>
            algorithm.lambda_ModifiedSubGraph_MPBI_IterWithoutPartition(pid, iter)(iterTimes, partitionSplit, ifFilteredCounter))

        }
        // skip merging to graph
        else{
          modifiedSubGraph = g.triplets.mapPartitionsWithIndex((pid, iter) =>
            algorithm.lambda_ModifiedSubGraph_MPBI_skipStep(pid, iter)(iterTimes, partitionSplit, ifFilteredCounter))
        }
      }

      // combine the vertex messages through partitions
      vertexModified = modifiedSubGraph.reduceByKey((v1, v2)=>
        algorithm.lambda_ReduceByKey(v1, v2)).cache()

      iterTimes = iterTimes + 1

      // get the amount of active vertices
      activeMessages = vertexModified.count()

      /*
      something while need to repartition
       */

      val endTime = System.nanoTime()
      //val endNew = System.nanoTime()
      //println("-------------------------")
      println("Whole iteration time: " + (endTime - startTime))

      oldVertexModified.unpersist(blocking = false)
      if(afterCounter != beforeCounter){
        prevG.unpersistVertices(blocking = false)
        prevG.edges.unpersist(blocking = false)
      }
      beforeCounter = tempBeforeCounter
      afterCounter = ifFilteredCounter.sum
    }

    g = GraphXModified.joinVerticesDefault(g, vertexModified)((vid, v1, v2) =>
      algorithm.lambda_JoinVerticesDefaultFirst(vid, v1, v2))(vAttr =>
      algorithm.lambda_JoinVerticesDefaultSecond(vAttr))
      .cache()

    // extract the remained data
    modifiedSubGraph = g.triplets.mapPartitionsWithIndex((pid, iter) =>
      algorithm.lambda_ModifiedSubGraph_MPBI_All(pid, iter)(iterTimes, partitionSplit, ifFilteredCounter))

    vertexModified = modifiedSubGraph.reduceByKey((v1, v2)=>
      algorithm.lambda_ReduceByKey(v1, v2)).cache()

    // the final combine
    g = GraphXModified.joinVerticesDefault(g, vertexModified)((vid, v1, v2) =>
      algorithm.lambda_JoinVerticesDefaultFirst(vid, v1, v2))(vAttr =>
      algorithm.lambda_JoinVerticesDefaultSecond(vAttr))
      .cache()
    vertexModified.unpersist(blocking = false)
    g

  }

  // after running algorithm, close the server
  def close[VD: ClassTag, ED: ClassTag](Graph: Graph[VD, ED]):Unit = {
    Graph.vertices.foreachPartition(g=>{
      val Process = new GPUNative
      var envInit : Boolean = false
      val pid = TaskContext.getPartitionId()
      while(! envInit){
        envInit = Process.GPUShutdown(pid)
      }
    })
  }
}

