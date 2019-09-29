package edu.ustc.nodb.PregelGPU

import java.io.{File, PrintWriter}

import edu.ustc.nodb.PregelGPU.template.lambdaTemplete

import edu.ustc.nodb.PregelGPU.plugin.checkPointer.{PeriodicGraphCheckpointer, PeriodicRDDCheckpointer}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, TaskContext}

import scala.reflect.ClassTag

object PregelGPU{

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

    var beforeCounter = ifFilteredCounter.value

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

      // merge the messages into graph
      g = g.joinVertices(messages)((vid, v1, v2) =>
        algorithm.lambda_globalVertexFunc(vid, v1, v2))

      graphCheckPointer.update(g)

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
      /*
      g.triplets.foreachPartition(iter => {
        val pid = TaskContext.getPartitionId()
        var temp : EdgeTriplet[VD, ED]  = null

        val writer = new PrintWriter(new File("/home/liqi/IdeaProjects/GraphXwithGPU/logGPU/" +
            "testGPUEdgeLog_pid" + pid + "_iter" + iterTimes + ".txt"))
        while(iter.hasNext){
          temp = iter.next()
          var chars = ""
          chars = chars + " " + temp.srcId + " : " + temp.srcAttr
          chars = chars + " -> " + temp.dstId + " : " + temp.dstAttr
          chars = chars + " Edge attr: " + temp.attr
          writer.write("In iter " + iterTimes + " of part" + pid + " , edge data: "
            + chars + '\n')

        }
        writer.close()

      })

      println("*----------------------------------------------*")
      g.vertices.foreachPartition(iter => {
        val pid = TaskContext.getPartitionId()
        var temp : (VertexId, VD)  = null
        val writer = new PrintWriter(new File("/home/liqi/IdeaProjects/GraphXwithGPU/logGPU/" +
          "testGPUVertexLog_pid" + pid + "_iter" + iterTimes + ".txt"))
        while(iter.hasNext){
          temp = iter.next()
          var chars = ""
          chars = chars + " " + temp._1 + " : " + temp._2
          writer.write("In iter " + iterTimes + " of part " + pid + " , vertex data: "
            + chars + '\n')
        }
        writer.close()
      })
      println("*----------------------------------------------*")
      */
      if(envControl.runningInSkip){
        if(afterCounter == beforeCounter){
          // skip getting vertex information through graph
          messages = GraphXUtils.mapReduceTripletsIntoGPU_Skipping(g, ifFilteredCounter,
            algorithm.lambda_GPUExecute_skipStep, algorithm.lambda_globalReduceFunc)

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
        }
        else{
          // run the main process
          messages = GraphXUtils.mapReduceTripletsIntoGPU(g, ifFilteredCounter,
            algorithm.lambda_GPUExecute, algorithm.lambda_globalReduceFunc,
            Some((oldMessages, activeDirection)))
        }
      }
      else{
        // run the main process
        messages = GraphXUtils.mapReduceTripletsIntoGPU(g, ifFilteredCounter,
          algorithm.lambda_GPUExecute, algorithm.lambda_globalReduceFunc,
          Some((oldMessages, activeDirection)))
      }

      messageCheckPointer.update(messages.asInstanceOf[RDD[(VertexId, A)]])

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

    if(envControl.runningInSkip){

      // in order to get all vertices information through graph
      // when the last step skipped the sync
      // here g.vertices stands for regarding all the vertices as activated
      messages = GraphXUtils.mapReduceTripletsIntoGPU_FinalCollect(g,
        algorithm.lambda_GPUExecute_finalCollect, algorithm.lambda_globalReduceFunc)

      g = g.joinVertices(messages)((vid, v1, v2) =>
        algorithm.lambda_globalVertexFunc(vid, v1, v2))

      graphCheckPointer.update(g)

    }

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
