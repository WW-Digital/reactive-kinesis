/*
 * Copyright 2017 WeightWatchers
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.weightwatchers.reactive.kinesis.consumer

import java.io.File
import java.util

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import com.amazonaws.auth.{
  AWSCredentialsProviderChain,
  DefaultAWSCredentialsProviderChain,
  EnvironmentVariableCredentialsProvider
}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.typesafe.config.ConfigFactory
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer.ConsumerConf
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration.DurationInt

//scalastyle:off magic.number
class KinesisConsumerSpec
    extends TestKit(ActorSystem("consumer-spec"))
    with ImplicitSender
    with FreeSpecLike
    with Matchers
    with MockitoSugar
    with BeforeAndAfterAll
    with GivenWhenThen
    with ScalaFutures {

  val defaultKinesisConfig =
    ConfigFactory
      .parseFile(new File("modules/core/src/main/resources/reference.conf"))
      .getConfig("kinesis")

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(3, Seconds), interval = Span(50, Millis))

  val kinesisConfig = ConfigFactory
    .parseString(
      """
        |kinesis {
        |
        |   application-name = "TestSpec"
        |
        |   testConsumer-1 {
        |      stream-name = "test-kinesis-reliability"
        |
        |      worker {
        |         batchTimeoutSeconds = 1234
        |         gracefulShutdownHook = false
        |         shutdownTimeoutSeconds = 2
        |      }
        |
        |      checkpointer {
        |         backoffMillis = 4321
        |      }
        |
        |      kcl {
        |         AWSCredentialsProvider = EnvironmentVariableCredentialsProvider
        |         regionName = us-east-1
        |         KinesisEndpoint = "CustomKinesisEndpoint"
        |         DynamoDBEndpoint = "CustomDynamoDBEndpoint"
        |         SkipShardSyncAtStartupIfLeasesExist = true
        |         TableName = "TableName"
        |      }
        |   }
        |
        |   testConsumer-2 {
        |      stream-name = "some-other-stream"
        |
        |      worker {
        |         failedMessageRetries = 3
        |         gracefulShutdownHook = false
        |      }
        |
        |      checkpointer {
        |         backoffMillis = 111
        |      }
        |
        |      kcl {
        |         AWSCredentialsProvider = DefaultAWSCredentialsProviderChain
        |         regionName = us-east-2
        |      }
        |   }
        |
        |}
      """.stripMargin
    )
    .getConfig("kinesis")
    .withFallback(defaultKinesisConfig)

  "The KinesisConsumer" - {

    "Should create an instance of KinesisConsumer" in {
      val kinesisConsumer =
        KinesisConsumer(ConsumerConf(kinesisConfig, "testConsumer-1"), TestProbe().ref, system)

      kinesisConsumer should not be null // scalastyle:ignore

      kinesisConsumer.recordProcessorFactory
        .createProcessor() shouldBe a[ConsumerProcessingManager]
    }

    "Should parse the Config into a ConsumerConf for a single consumer" in {
      val consumerConf = ConsumerConf(kinesisConfig, "testConsumer-1")

      consumerConf.workerConf.batchTimeout should be(1234.seconds)
      consumerConf.workerConf.failedMessageRetries should be(1)
      consumerConf.workerConf.failureTolerancePercentage should be(0.25)
      consumerConf.workerConf.shutdownHook should be(false)
      consumerConf.workerConf.shutdownTimeout should be(Timeout(2.seconds))
      consumerConf.checkpointerConf.backoff should be(4321.millis)
      consumerConf.checkpointerConf.interval should be(2000.millis)          //reference default
      consumerConf.checkpointerConf.notificationDelay should be(1000.millis) //reference default
      consumerConf.dispatcher should be(Some("kinesis.akka.default-dispatcher"))
      consumerConf.kclConfiguration.getApplicationName should be(
        "TestSpec-test-kinesis-reliability"
      )
      consumerConf.kclConfiguration.getStreamName should be("test-kinesis-reliability")
      consumerConf.kclConfiguration.getKinesisEndpoint should be("CustomKinesisEndpoint")
      consumerConf.kclConfiguration.getDynamoDBEndpoint should be("CustomDynamoDBEndpoint")
      consumerConf.kclConfiguration.getSkipShardSyncAtWorkerInitializationIfLeasesExist should be(
        true
      )
      consumerConf.kclConfiguration.getTableName should be("TableName")

      val credentialsProvider = consumerConf.kclConfiguration.getKinesisCredentialsProvider
        .asInstanceOf[AWSCredentialsProviderChain]
      //access private field
      val field = classOf[AWSCredentialsProviderChain].getDeclaredField("credentialsProviders")
      field.setAccessible(true)
      field
        .get(credentialsProvider)
        .asInstanceOf[util.LinkedList[_]]
        .getFirst shouldBe a[EnvironmentVariableCredentialsProvider]

      consumerConf.kclConfiguration.getRegionName should be("us-east-1")
    }

    "Should parse the Config into multiple ConsumerConf objects for multiple consumers" in {
      val consumerConf2 = ConsumerConf(kinesisConfig, "testConsumer-2")

      consumerConf2.workerConf.batchTimeout should be(10.seconds)
      consumerConf2.workerConf.failedMessageRetries should be(3)
      consumerConf2.workerConf.failureTolerancePercentage should be(0.25)
      consumerConf2.checkpointerConf.backoff should be(111.millis)
      consumerConf2.checkpointerConf.interval should be(2000.millis)          //reference default
      consumerConf2.checkpointerConf.notificationDelay should be(1000.millis) //reference default
      consumerConf2.dispatcher should be(Some("kinesis.akka.default-dispatcher"))
      consumerConf2.kclConfiguration.getApplicationName should be("TestSpec-some-other-stream")
      consumerConf2.kclConfiguration.getStreamName should be("some-other-stream")

      val credentialsProvider2 = consumerConf2.kclConfiguration.getKinesisCredentialsProvider
        .asInstanceOf[AWSCredentialsProviderChain]
      //access private field
      val field2 = classOf[AWSCredentialsProviderChain].getDeclaredField("credentialsProviders")
      field2.setAccessible(true)
      field2
        .get(credentialsProvider2)
        .asInstanceOf[util.LinkedList[_]]
        .getFirst shouldBe a[DefaultAWSCredentialsProviderChain]

      consumerConf2.kclConfiguration.getRegionName should be("us-east-2")
    }

    "Should create and run multiple workers, returning Futures which don't complete until the Worker has finished for multiple consumers" in {
      val worker1 = mock[Worker]
      val worker2 = mock[Worker]

      val consumer1 = new KinesisConsumer(ConsumerConf(kinesisConfig, "testConsumer-1"),
                                          null, // scalastyle:ignore
                                          system,
                                          system) {

        override lazy val kclWorker = worker1
      }

      val consumer2 = new KinesisConsumer(ConsumerConf(kinesisConfig, "testConsumer-2"),
                                          null, // scalastyle:ignore
                                          system,
                                          system) {

        override lazy val kclWorker = worker2
      }

      Given("Two Workers which run for 250 ms each")
      Mockito
        .when(worker1.run())
        .thenAnswer(new Answer[Unit] {
          override def answer(invocation: InvocationOnMock): Unit = {
            Thread.sleep(250)
          }
        })
      Mockito
        .when(worker2.run())
        .thenAnswer(new Answer[Unit] {
          override def answer(invocation: InvocationOnMock): Unit = {
            Thread.sleep(250)
          }
        })

      When("We start the Consumers")
      val future1 = consumer1.start()
      val future2 = consumer2.start()

      Then("It shouldn't complete immediately")
      future1.isCompleted shouldBe false
      future2.isCompleted shouldBe false

      whenReady(future1) { _ =>
        succeed
      }

      whenReady(future2) { _ =>
        succeed
      }
    }

    "Should create and run the worker, returning a failed Future" in {
      val worker = mock[Worker]
      val consumer = new KinesisConsumer(ConsumerConf(kinesisConfig, "testConsumer-1"),
                                         null, // scalastyle:ignore
                                         system,
                                         system) {

        override lazy val kclWorker = worker
      }

      Given("A Worker which throws an Exception")
      val exception = new RuntimeException("TEST")
      Mockito.when(worker.run()).thenThrow(exception)

      When("We start the Consumer")
      val future = consumer.start()

      Then("It should complete with a failed future")
      whenReady(future.failed) { t =>
        t shouldBe exception
      }
    }

    "Should call requestShutdown on worker, when stop is called for consumer" in {
      val worker = mock[Worker]
      Mockito
        .when(worker.startGracefulShutdown())
        .thenReturn(mock[java.util.concurrent.Future[java.lang.Boolean]])

      Given("A running consumer")

      val consumer = new KinesisConsumer(ConsumerConf(kinesisConfig, "testConsumer-1"),
                                         null, // scalastyle:ignore
                                         system,
                                         system) {

        override lazy val kclWorker = worker
      }

      When("We start the consumer")
      val future = consumer.start()

      whenReady(future) { _ =>
        And("Stop the consumer")
        consumer.stop()

        Then("It should call request shutdown on worker once")
        Mockito.verify(worker, Mockito.times(1)).startGracefulShutdown()
      }
    }

    "Should only call shutdown once, even on multiple invocations of stop" in {
      val worker = mock[Worker]

      Given("A consumer with our mocked worker")
      Mockito
        .when(worker.startGracefulShutdown())
        .thenReturn(mock[java.util.concurrent.Future[java.lang.Boolean]])

      val consumer = new KinesisConsumer(ConsumerConf(kinesisConfig, "testConsumer-1"),
                                         null, // scalastyle:ignore
                                         system,
                                         system) {

        override lazy val kclWorker = worker
      }

      When("We start the consumer")
      val future = consumer.start()

      whenReady(future) { _ =>
        And("We stop consumer twice")
        consumer.stop()
        consumer.stop()

        Then("It should only call request shutdown on worker once")
        Mockito.verify(worker, Mockito.times(1)).startGracefulShutdown()
      }
    }
  }
}

//scalastyle:on
