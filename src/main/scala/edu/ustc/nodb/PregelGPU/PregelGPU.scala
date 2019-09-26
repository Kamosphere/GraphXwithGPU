package edu.ustc.nodb.PregelGPU

import java.io.{File, PrintWriter}

import edu.ustc.nodb.PregelGPU.algorithm.lambdaTemplate
import edu.ustc.nodb.PregelGPU.plugin.GraphXModified
import edu.ustc.nodb.PregelGPU.plugin.checkPointer.{PeriodicGraphCheckpointer, PeriodicRDDCheckpointer}
import edu.ustc.nodb.PregelGPU.plugin.partitionStrategy.{EdgePartition1DReverse, EdgePartitionNumHookedTest, EdgePartitionPreSearch}
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

    val checkpointInterval = graph.vertices.sparkContext.getConf
      .getInt("spark.graphx.pregel.checkpointInterval", -1)

    val startTime = System.nanoTime()
    // initiate the graph
    val spGraph = graph.mapVertices ((vid, attr) =>
      algorithm.lambda_initGraph(vid, attr))
    // var g = algorithm.repartition(spGraph)
    var g = spGraph
    val graphCheckpointer = new PeriodicGraphCheckpointer[(Boolean, VD), ED](
      checkpointInterval, graph.vertices.sparkContext)
    graphCheckpointer.update(g)
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
      algorithm.lambda_ReduceByKey)

    val messageCheckpointer = new PeriodicRDDCheckpointer[(VertexId, (Boolean, VD))](
      checkpointInterval, graph.vertices.sparkContext)
    messageCheckpointer.update(vertexModified.asInstanceOf[RDD[(VertexId, (Boolean, VD))]])

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
      graphCheckpointer.update(g)

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

        g.triplets.foreachPartition(iter => {
          val pid = TaskContext.getPartitionId()
          var temp : EdgeTriplet[(Boolean, VD),ED]  = null

          val writer = new PrintWriter(new File("/home/liqi/IdeaProjects/GraphXwithGPU/logGPU/" +
            "testGPUEdgeLog_pid" + pid + "_iter" + iterTimes + ".txt"))
          while(iter.hasNext){
            temp = iter.next()
            var chars = ""
            chars = chars + " " + temp.srcId + " : " + temp.srcAttr._1 + " :: " + temp.srcAttr._2
            chars = chars + " -> " + temp.dstId + " : " + temp.dstAttr._1 + " :: " + temp.dstAttr._2
            chars = chars + " Edge attr: " + temp.attr
            writer.write("In iter " + iterTimes + " of part" + pid + " , edge data: "
              + chars + '\n')

          }
          writer.close()

        })

        println("*----------------------------------------------*")
        g.vertices.foreachPartition(iter => {
          val pid = TaskContext.getPartitionId()
          var temp : (VertexId, (Boolean, VD))  = null
          val writer = new PrintWriter(new File("/home/liqi/IdeaProjects/GraphXwithGPU/logGPU/" +
            "testGPUVertexLog_pid" + pid + "_iter" + iterTimes + ".txt"))
          while(iter.hasNext){
            temp = iter.next()
            var chars = ""
            chars = chars + " " + temp._1 + " : " + temp._2._1 + " :: " + temp._2._2
            writer.write("In iter " + iterTimes + " of part " + pid + " , vertex data: "
              + chars + '\n')
          }
          writer.close()
        })
        println("*----------------------------------------------*")
        if(envControl.runningInSkip){

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

        else{

          modifiedSubGraph = GraphXModified.msgExtract(g,
            Some(oldVertexModified, EdgeDirection.Either))
            .mapPartitionsWithIndex((pid, iter) =>
              algorithm.lambda_ModifiedSubGraph_normalIter(pid, iter)(
                iterTimes, partitionSplit, ifFilteredCounter))

        }
      }

      // distribute the vertex messages into partitions
      vertexModified = g.vertices.aggregateUsingIndex(modifiedSubGraph,
        algorithm.lambda_ReduceByKey)
      messageCheckpointer.update(vertexModified)

      /*
      vertexModified.foreachPartition(i =>{
        val pid = TaskContext.getPartitionId()
        var chars = ""
        var count = 0
        var temp : (VertexId, (Boolean, VD)) = null

        while(i.hasNext){
          temp = i.next()
          chars = chars + " " + temp._1 + " "
          count = count + 1
        }

        println("In iter " + iterTimes + " of part" + pid + ", next active vertex count: "
          + count + " and they are: " + chars)
      })
      */

      // get the amount of active vertices
      activeMessages = vertexModified.count()

      /*
      something while need to repartition
       */

      oldVertexModified.unpersist(blocking = false)
      if(afterCounter != beforeCounter) {
        prevG.unpersistVertices(blocking = false)
        prevG.edges.unpersist(blocking = false)
      }
      beforeCounter = tempBeforeCounter
      afterCounter = ifFilteredCounter.sum

      val endTime = System.nanoTime()

      println("Whole iteration time: " + (endTime - startTime) +
        ", next iter active node amount: " + activeMessages)
      println("-------------------------")

      iterTimes = iterTimes + 1

    }

    g = GraphXModified.joinVerticesOrDeactivate(g, vertexModified)((vid, v1, v2) =>
      algorithm.lambda_JoinVerticesDefault(vid, v1, v2))(vAttr =>
      (false, vAttr._2))
    graphCheckpointer.update(g)

    // extract the remained data
    modifiedSubGraph = g.triplets.mapPartitionsWithIndex((pid, iter) =>
      algorithm.lambda_modifiedSubGraph_collectAll(pid, iter)(
        iterTimes, partitionSplit, ifFilteredCounter))

    vertexModified = g.vertices.aggregateUsingIndex(modifiedSubGraph,
      algorithm.lambda_ReduceByKey)
    messageCheckpointer.update(vertexModified)

    // the final combine
    g = GraphXModified.joinVerticesOrDeactivate(g, vertexModified)((vid, v1, v2) =>
      algorithm.lambda_JoinVerticesDefault(vid, v1, v2))(vAttr =>
      (false, vAttr._2))
    graphCheckpointer.update(g)

    messageCheckpointer.unpersistDataSet()
    graphCheckpointer.deleteAllCheckpoints()
    messageCheckpointer.deleteAllCheckpoints()
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
