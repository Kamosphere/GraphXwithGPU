package edu.ustc.nodb.PregelGPU

import edu.ustc.nodb.PregelGPU.plugin.checkPointer.{PeriodicGraphCheckpointer, PeriodicRDDCheckpointer}
import edu.ustc.nodb.PregelGPU.template.lambdaTemplete
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, TaskContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object PregelGPUSkipping{

  // scalastyle:off println

  def run[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (graph: Graph[VertexId, ED],
   activeDirection: EdgeDirection = EdgeDirection.Either,
   maxIterations: Int = Int.MaxValue)
  (algorithm: lambdaTemplete[VD, ED, A])
  : Graph[VD, ED] = {

    val startTime = System.nanoTime()

    val checkpointInterval = graph.vertices.sparkContext.getConf
      .getInt("spark.graphx.pregel.checkpointInterval", -1)

    // initiate the graph
    val spGraph = graph.mapVertices ((vid, attr) =>
      algorithm.lambda_initGraph(vid, attr))
    // var g = algorithm.repartition(spGraph)
    var g = spGraph
    val graphCheckPointer = new PeriodicGraphCheckpointer[VD, ED](
      checkpointInterval, graph.vertices.sparkContext)
    graphCheckPointer.update(g)

    val countOutDegree = g.outDegrees.collectAsMap()

    val sc = org.apache.spark.SparkContext.getOrCreate()

    val ifFilteredCounter = sc.longAccumulator("ifFilteredCounter")

    var beforeCounter = -1L

    var iterTimes = 0

    var partitionSplit = g.innerVerticesEdgesCount().collectAsMap()

    // Input vertex count and edge count in order to init GPU env
    algorithm.fillPartitionInnerData(partitionSplit)

    // Input edge data into GPU
    g.edges.foreachPartition(iter => {
      val pid = TaskContext.getPartitionId()
      algorithm.lambda_edgeImport(pid, iter)(
        iterTimes, countOutDegree, ifFilteredCounter)
    })

    val iterVertexInfo = g.vertices.map(vertex => {
      (vertex._1, (algorithm.lambda_initMessage(vertex._1), 0))
    })

    var iterUpdate = g.vertices.aggregateUsingIndex(iterVertexInfo,
      (a: (A, Int), b: (A, Int)) => b).cache()

    var messages = GraphXUtils.mapReduceTripletsIntoGPU(g, ifFilteredCounter,
      algorithm.lambda_GPUExecute, algorithm.lambda_globalReduceFunc)

    val messageCheckPointer = new PeriodicRDDCheckpointer[(VertexId, A)](
      checkpointInterval, graph.vertices.sparkContext)
    messageCheckPointer.update(messages.asInstanceOf[RDD[(VertexId, A)]])

    // get the amount of active vertices
    var activeMessages = messages.count()

    var afterCounter = ifFilteredCounter.value

    var prevIterSkipped = false

    iterTimes = 1
    var prevG : Graph[VD, ED] = null

    val ifRepartition = false

    // loop
    while(activeMessages > 0 && iterTimes < maxIterations) {

      val startTime = System.nanoTime()

      prevG = g

      ifFilteredCounter.reset()

      // Should be 0
      // if a partition not satisfied skipping situation, counter will be added
      // so if afterCounter is 0, all the partition could skip sync
      val tempBeforeCounter = ifFilteredCounter.sum

      if(! prevIterSkipped){

        // merge the messages into graph
        g = g.joinVertices(messages)((vid, v1, v2) =>
          algorithm.lambda_globalVertexFunc(vid, v1, v2))

        graphCheckPointer.update(g)
      }

      val oldMessages = messages

      if(ifRepartition) {

        partitionSplit = g.innerVerticesEdgesCount().collectAsMap()

        algorithm.fillPartitionInnerData(partitionSplit)

        g.triplets.foreachPartition(iter => {
          val pid = TaskContext.getPartitionId()
          algorithm.lambda_edgeImport(pid, iter)(
            iterTimes, countOutDegree, ifFilteredCounter)
        })
      }

      if(afterCounter == beforeCounter){
        // skip getting vertex information through graph
        messages = GraphXUtils.mapReduceTripletsIntoGPU_Skipping(g, ifFilteredCounter,
          algorithm.lambda_GPUExecute_skipStep, algorithm.lambda_globalReduceFunc)

        messageCheckPointer.update(messages.asInstanceOf[RDD[(VertexId, A)]])

        iterUpdate = iterUpdate.leftZipJoin(messages)((vid, updateSource, message) => {
          if (message.nonEmpty) (message.get, iterTimes)
          else updateSource
        }).cache()

        // to let the next iter know
        prevIterSkipped = true
      }
      else if (prevIterSkipped){
        // run the main process
        // if the prev iter skipped the sync, the iter need to catch all data
        val prevSkippingDirection : EdgeDirection = null
        messages = GraphXUtils.mapReduceTripletsIntoGPU(g, ifFilteredCounter,
          algorithm.lambda_GPUExecute, algorithm.lambda_globalReduceFunc,
          Some((oldMessages, prevSkippingDirection)))

        messageCheckPointer.update(messages.asInstanceOf[RDD[(VertexId, A)]])

        iterUpdate = iterUpdate.leftZipJoin(messages)((vid, updateSource, message) => {
          if (message.nonEmpty) (message.get, iterTimes)
          else updateSource
        }).cache()

        val delayedUpdateInfo = iterUpdate.mapPartitions(i => {
          val filteredResult = new ArrayBuffer[(VertexId, A)]
          var temp : (VertexId, (A, Int)) = null
          while (i.hasNext) {
            temp = i.next()
            if(temp._2._2 < iterTimes) {
              filteredResult. += ((temp._1, temp._2._1))
            }
          }
          filteredResult.iterator
        }).partitionBy(g.vertices.partitioner.get).cache()

        val delayedUpdateMessage = g.vertices.aggregateUsingIndex(
          delayedUpdateInfo, algorithm.lambda_globalReduceFunc).cache()

        // merge the messages into graph
        g = g.joinVertices(delayedUpdateMessage)((vid, v1, v2) =>
          algorithm.lambda_globalVertexFunc(vid, v1, v2))

        graphCheckPointer.update(g)

        prevIterSkipped = false
      }
      else{
        // run the main process
        messages = GraphXUtils.mapReduceTripletsIntoGPU(g, ifFilteredCounter,
          algorithm.lambda_GPUExecute, algorithm.lambda_globalReduceFunc,
          Some((oldMessages, activeDirection)))

        messageCheckPointer.update(messages.asInstanceOf[RDD[(VertexId, A)]])

        iterUpdate = iterUpdate.leftZipJoin(messages)((vid, updateSource, message) => {
          if (message.nonEmpty) (message.get, iterTimes)
          else updateSource
        }).cache()

        prevIterSkipped = false
      }

      // get the amount of active vertices
      activeMessages = messages.count()

      /*
      something while need to repartition
       */

      oldMessages.unpersist(blocking = false)

      // Not skipping
      if(afterCounter != beforeCounter) {
        prevG.unpersistVertices(blocking = false)
        prevG.edges.unpersist(blocking = false)
      }

      // save for detecting if next iter can skip
      beforeCounter = tempBeforeCounter
      afterCounter = ifFilteredCounter.sum

      val endTime = System.nanoTime()

      println("Whole iteration time: " + (endTime - startTime) +
        ", next iter active node amount: " + activeMessages)
      println("-------------------------")

      iterTimes = iterTimes + 1

    }

    // in order to get all vertices information through graph
    // when the last step skipped the sync
    // here g.vertices stands for regarding all the vertices as activated
    messages = GraphXUtils.mapReduceTripletsIntoGPU_FinalCollect(g,
      algorithm.lambda_GPUExecute_finalCollect, algorithm.lambda_globalReduceFunc)

    g = g.joinVertices(messages)((vid, v1, v2) =>
      algorithm.lambda_globalVertexFunc(vid, v1, v2))

    graphCheckPointer.update(g)

    iterUpdate.unpersist(false)
    messageCheckPointer.unpersistDataSet()
    graphCheckPointer.deleteAllCheckpoints()
    messageCheckPointer.deleteAllCheckpoints()

    val endTime = System.nanoTime()

    println("The whole Pregel process time: " + (endTime - startTime))
    g

  }

  // after running algorithm, close the server
  def close[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (Graph: Graph[VD, ED], algorithm: lambdaTemplete[VD, ED, A]): Unit = {

    Graph.vertices.foreachPartition(g => {
      val pid = TaskContext.getPartitionId()
      algorithm.lambda_shutDown(pid, g)
    })
  }

  // scalastyle:on println
}
