package edu.ustc.nodb.PregelGPU


import edu.ustc.nodb.PregelGPU.Algorithm.SSSP.{GPUController, GPUNative}
import edu.ustc.nodb.PregelGPU.Algorithm.SSSPshm.GPUControllerShm
import edu.ustc.nodb.PregelGPU.Algorithm.lambdaTemplete
import edu.ustc.nodb.PregelGPU.Plugin.GraphXModified
import edu.ustc.nodb.PregelGPU.Plugin.partitionStrategy.EdgePartition1DReverse
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object PregelInGPU{

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VertexId, ED],
                                      maxIterations: Int = Int.MaxValue)
                                     (algorithm: lambdaTemplete[VD, ED])
  : Graph[VD, ED] = {

    // initiate the graph
    val spGraph = graph.mapVertices ((vid, attr) => algorithm.lambda_initGraph(vid, attr)).partitionBy(EdgePartition1DReverse).cache()

    val startTimeB = System.nanoTime()

    var g = spGraph

    val endTimeB = System.nanoTime()

    println("Pre partition time: " + (endTimeB - startTimeB))

    val countOutDegree = g.outDegrees.collectAsMap()

    val sc = org.apache.spark.SparkContext.getOrCreate()

    val ifFilteredCounter = sc.longAccumulator("ifFilteredCounter")

    var beforeCounter = ifFilteredCounter.value

    var iterTimes = 0

    var partitionSplit : collection.Map[Int,(Int, Int)] = g.triplets.mapPartitionsWithIndex((pid, Iter)=>{
      algorithm.lambda_partitionSplit(pid, Iter)
    }).collectAsMap()

    var modifiedSubGraph: RDD[(VertexId, VD)] = g.triplets.mapPartitionsWithIndex((pid, iter) =>
      algorithm.lambda_ModifiedSubGraph_repartitionIter(pid, iter)(iterTimes, countOutDegree, partitionSplit, ifFilteredCounter))

    // get the vertex number and edge number in every partition
    // in order to avoid allocation arraycopy

    // combine the vertex messages through partitions
    var vertexModified = g.vertices.aggregateUsingIndex(modifiedSubGraph,
      algorithm.lambda_ReduceByKey).cache()

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

      //combine the messages with graph
      g = GraphXModified.joinVerticesOrModify(g, vertexModified)((vid, v1, v2) =>
        algorithm.lambda_JoinVerticesDefaultFirst(vid, v1, v2))(vAttr =>
        algorithm.lambda_JoinVerticesDefaultSecond(vAttr))
        .cache()

      if(ifRepartition){

        partitionSplit = g.triplets.mapPartitionsWithIndex((pid, Iter)=>{
          algorithm.lambda_partitionSplit(pid, Iter)
        }).collectAsMap()

        modifiedSubGraph = g.triplets.mapPartitionsWithIndex((pid, iter) =>{
          algorithm.lambda_ModifiedSubGraph_repartitionIter(pid, iter)(iterTimes, countOutDegree, partitionSplit, ifFilteredCounter)
        })
      }
      else{
        if(afterCounter != beforeCounter){
/*
          modifiedSubGraph = g.triplets.mapPartitionsWithIndex((pid, iter) =>
            algorithm.lambda_ModifiedSubGraph_MPBI_IterWithoutPartition(pid, iter)(iterTimes, partitionSplit, ifFilteredCounter))
*/
          //run the main process

          modifiedSubGraph = GraphXModified.msgExtract(g, Some(oldVertexModified, EdgeDirection.Either)).mapPartitionsWithIndex((pid, iter) =>
            algorithm.lambda_ModifiedSubGraph_normalIter(pid, iter)(iterTimes, partitionSplit, ifFilteredCounter))

        }
        // skip getting vertex information through graph
        else{

          modifiedSubGraph = g.triplets.mapPartitionsWithIndex((pid, iter) =>
            algorithm.lambda_modifiedSubGraph_skipStep(pid, iter)(iterTimes, partitionSplit, ifFilteredCounter))
        }
      }

      // distribute the vertex messages into partitions
      vertexModified = g.vertices.aggregateUsingIndex(modifiedSubGraph,
        algorithm.lambda_ReduceByKey).cache()

      iterTimes = iterTimes + 1

      // get the amount of active vertices
      activeMessages = vertexModified.count()

      /*
      something while need to repartition
       */

      val endTime = System.nanoTime()

      println("Whole iteration time: " + (endTime - startTime))
      println("-------------------------")

      oldVertexModified.unpersist(blocking = false)
      if(afterCounter != beforeCounter){
        prevG.unpersistVertices(blocking = false)
        prevG.edges.unpersist(blocking = false)
      }
      beforeCounter = tempBeforeCounter
      afterCounter = ifFilteredCounter.sum


    }

    g = GraphXModified.joinVerticesOrModify(g, vertexModified)((vid, v1, v2) =>
      algorithm.lambda_JoinVerticesDefaultFirst(vid, v1, v2))(vAttr =>
      algorithm.lambda_JoinVerticesDefaultSecond(vAttr))
      .cache()

    // extract the remained data
    modifiedSubGraph = g.triplets.mapPartitionsWithIndex((pid, iter) =>
      algorithm.lambda_modifiedSubGraph_collectAll(pid, iter)(iterTimes, partitionSplit, ifFilteredCounter))

    vertexModified = g.vertices.aggregateUsingIndex(modifiedSubGraph,
      algorithm.lambda_ReduceByKey).cache()

    // the final combine
    g = GraphXModified.joinVerticesOrModify(g, vertexModified)((vid, v1, v2) =>
      algorithm.lambda_JoinVerticesDefaultFirst(vid, v1, v2))(vAttr =>
      algorithm.lambda_JoinVerticesDefaultSecond(vAttr))
      .cache()
    vertexModified.unpersist(blocking = false)
    g

  }

  // after running algorithm, close the server
  def close[VD: ClassTag, ED: ClassTag](Graph: Graph[VD, ED], algorithm: lambdaTemplete[VD, ED]): Unit = {

    Graph.vertices.foreachPartition(g => {
      val pid = TaskContext.getPartitionId()
      algorithm.lambda_shutDown(pid, g)
    })
  }
}

object envControl{
  // 0 for server, other for local
  val controller : Int = 1
}