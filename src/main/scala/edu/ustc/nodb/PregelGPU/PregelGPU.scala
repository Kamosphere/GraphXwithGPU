package edu.ustc.nodb.PregelGPU

import edu.ustc.nodb.PregelGPU.algorithm.lambdaTemplate
import edu.ustc.nodb.PregelGPU.plugin.GraphXModified
import edu.ustc.nodb.PregelGPU.plugin.partitionStrategy.{EdgePartition1DReverse, EdgePartitionPreSearch}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, TaskContext}

import scala.reflect.ClassTag

object PregelGPU{

  // scalastyle:off println

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VertexId, ED],
                                      maxIterations: Int = Int.MaxValue)
                                     (algorithm: lambdaTemplate[VD, ED])
  : Graph[(Boolean, VD), ED] = {

    // initiate the graph
    val spGraph = graph.mapVertices ((vid, attr) =>
      algorithm.lambda_initGraph(vid, attr)).partitionBy(EdgePartition1DReverse).cache()

    val startTime = System.nanoTime()
    // var g = algorithm.repartition(spGraph)
    var g = spGraph
    val endTime = System.nanoTime()

    println("prePartition Time : " + (endTime - startTime))
    val countOutDegree = g.outDegrees.collectAsMap()

    val sc = org.apache.spark.SparkContext.getOrCreate()

    val ifFilteredCounter = sc.longAccumulator("ifFilteredCounter")

    var beforeCounter = ifFilteredCounter.value

    var iterTimes = 0

    var partitionSplit : collection.Map[Int, (Int, Int)] =
      g.triplets.mapPartitionsWithIndex((pid, Iter) => {
        algorithm.lambda_partitionSplit(pid, Iter)
      }).collectAsMap()

    var modifiedSubGraph: RDD[(VertexId, (Boolean, VD))] = g.triplets.mapPartitionsWithIndex((pid, iter) =>
      algorithm.lambda_ModifiedSubGraph_repartitionIter(pid, iter)(
        iterTimes, countOutDegree, partitionSplit, ifFilteredCounter))

    // get the vertex number and edge number in every partition
    // in order to allocate

    // distribute the vertex messages by partition
    var vertexModified = g.vertices.aggregateUsingIndex(modifiedSubGraph,
      algorithm.lambda_ReduceByKey).cache()

    // get the amount of active vertices
    var activeMessages = vertexModified.count()

    var afterCounter = ifFilteredCounter.value

    iterTimes = 1
    var prevG : Graph[(Boolean, VD), ED] = null

    val ifRepartition = false

    // loop
    while(activeMessages > 0 && iterTimes < maxIterations) {

      val startTime = System.nanoTime()

      prevG = g
      val oldVertexModified = vertexModified
      ifFilteredCounter.reset()
      val tempBeforeCounter = ifFilteredCounter.sum

      // merge the messages into graph
      g = GraphXModified.joinVerticesOrDeactivate(g, vertexModified)((vid, v1, v2) =>
        algorithm.lambda_JoinVerticesDefault(vid, v1, v2))(vAttr =>
        (false, vAttr._2))
        .cache()

      if(ifRepartition) {

        partitionSplit = g.triplets.mapPartitionsWithIndex((pid, Iter) => {
          algorithm.lambda_partitionSplit(pid, Iter)
        }).collectAsMap()

        modifiedSubGraph = g.triplets.mapPartitionsWithIndex((pid, iter) => {
          algorithm.lambda_ModifiedSubGraph_repartitionIter(pid, iter)(
            iterTimes, countOutDegree, partitionSplit, ifFilteredCounter)
        })
      }
      else {
        if (afterCounter != beforeCounter) {
          // run the main process
          modifiedSubGraph = GraphXModified.msgExtract(g,
            Some(oldVertexModified, EdgeDirection.Either))
            .mapPartitionsWithIndex((pid, iter) =>
              algorithm.lambda_ModifiedSubGraph_normalIter(pid, iter)(
                iterTimes, partitionSplit, ifFilteredCounter))

        }
        // skip getting vertex information through graph
        else {
          modifiedSubGraph = g.triplets.mapPartitionsWithIndex((pid, iter) =>
            algorithm.lambda_modifiedSubGraph_skipStep(pid, iter)(
              iterTimes, partitionSplit, ifFilteredCounter))
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

      println("Whole iteration time: " + (endTime - startTime) +
        ", next iter active node amount: " + activeMessages)
      println("-------------------------")

      oldVertexModified.unpersist(blocking = false)
      if(afterCounter != beforeCounter) {
        prevG.unpersistVertices(blocking = false)
        prevG.edges.unpersist(blocking = false)
      }
      beforeCounter = tempBeforeCounter
      afterCounter = ifFilteredCounter.sum

    }

    g = GraphXModified.joinVerticesOrDeactivate(g, vertexModified)((vid, v1, v2) =>
      algorithm.lambda_JoinVerticesDefault(vid, v1, v2))(vAttr =>
      (false, vAttr._2))
      .cache()

    // extract the remained data
    modifiedSubGraph = g.triplets.mapPartitionsWithIndex((pid, iter) =>
      algorithm.lambda_modifiedSubGraph_collectAll(pid, iter)(
        iterTimes, partitionSplit, ifFilteredCounter))

    vertexModified = g.vertices.aggregateUsingIndex(modifiedSubGraph,
      algorithm.lambda_ReduceByKey).cache()

    // the final combine
    g = GraphXModified.joinVerticesOrDeactivate(g, vertexModified)((vid, v1, v2) =>
      algorithm.lambda_JoinVerticesDefault(vid, v1, v2))(vAttr =>
      (false, vAttr._2))
      .cache()
    vertexModified.unpersist(blocking = false)
    g

  }

  // after running algorithm, close the server
  def close[VD: ClassTag, ED: ClassTag]
  (Graph: Graph[(Boolean, VD), ED], algorithm: lambdaTemplate[VD, ED]): Unit = {

    Graph.vertices.foreachPartition(g => {
      val pid = TaskContext.getPartitionId()
      algorithm.lambda_shutDown(pid, g)
    })
  }

  // scalastyle:on println
}
