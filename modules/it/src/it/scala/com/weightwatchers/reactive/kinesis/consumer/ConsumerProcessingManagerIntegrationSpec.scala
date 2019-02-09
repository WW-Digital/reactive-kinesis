package com.weightwatchers.reactive.kinesis.consumer

import akka.actor.{ActorRef, Props}
import akka.testkit.TestProbe
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.weightwatchers.reactive.kinesis.common.{AkkaUnitTestLike, KinesisConfiguration, KinesisSuite}
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer.ConsumerConf
import org.scalatest.concurrent.Eventually
import org.scalatest.{FreeSpec, Matchers}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class ConsumerProcessingManagerIntegrationSpec
    extends FreeSpec
    with KinesisSuite
    with KinesisConfiguration
    with AkkaUnitTestLike
    with Matchers
    with Eventually {

  override def TestStreamNrOfMessagesPerShard: Long    = 0
  override def TestStreamNumberOfShards: Long          = 2
  override implicit def patienceConfig: PatienceConfig = PatienceConfig(60.seconds, 1.second)

  "A ConsumerProcessingManager on a stream with 2 shards" - {

    "should start and stop processing managers correctly" in new withKinesisConfForApp(
      "count_processing_managers"
    ) {
      // 2 consumers start - each operates on one shard
      val consumer1 = new TestKinesisConsumer(consumerConf(), TestProbe().testActor)
      val consumer2 = new TestKinesisConsumer(consumerConf(), TestProbe().testActor)
      eventually {
        consumer1.runningProcessingManagers shouldBe 1
        consumer2.runningProcessingManagers shouldBe 1
      }

      // if consumer1 stops, consumer2 should start a shard worker
      consumer1.stop()
      eventually {
        consumer1.runningProcessingManagers shouldBe 0
        consumer2.runningProcessingManagers shouldBe 2
      }

      // if a new consumer joins, consumer2 should shutdown one shard, which is then taken over by consumer3
      val consumer3 = new TestKinesisConsumer(consumerConf(), TestProbe().testActor)
      eventually {
        consumer1.runningProcessingManagers shouldBe 0
        consumer2.runningProcessingManagers shouldBe 1
        consumer3.runningProcessingManagers shouldBe 1
      }

      // if all consumers are stopped, all processing managers are shut down
      consumer2.stop()
      consumer3.stop()
      eventually {
        consumer1.runningProcessingManagers shouldBe 0
        consumer2.runningProcessingManagers shouldBe 0
        consumer3.runningProcessingManagers shouldBe 0
      }
    }
  }

  class TestKinesisConsumer(consumerConf: ConsumerConf, consumerWorkerProps: Props)
      extends KinesisConsumer(consumerConf, consumerWorkerProps, system, system) {
    def this(consumerConf: ConsumerConf, eventProcessor: ActorRef) =
      this(consumerConf,
           ConsumerWorker.props(eventProcessor,
                                consumerConf.workerConf,
                                consumerConf.checkpointerConf,
                                consumerConf.dispatcher))

    val createdProcessingManager = ListBuffer.empty[ConsumerProcessingManager]

    def runningProcessingManagers: Int =
      createdProcessingManager.count(_.shuttingDown.get == false)

    override private[consumer] val recordProcessorFactory: IRecordProcessorFactory =
      new IRecordProcessorFactory {
        override def createProcessor(): IRecordProcessor = {
          val manager = new ConsumerProcessingManager(
            system.actorOf(consumerWorkerProps, s"consumer-worker-${UUID_GENERATOR.generate()}"),
            kclWorker,
            managerBatchTimeout,
            consumerConf.workerConf.shutdownTimeout.duration
          )(ctx)
          createdProcessingManager += manager
          manager
        }
      }

    // test consumer are started during instantiation
    start()
  }
}
