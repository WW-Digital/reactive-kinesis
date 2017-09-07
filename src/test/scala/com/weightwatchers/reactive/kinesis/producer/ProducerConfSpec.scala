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

package com.weightwatchers.reactive.kinesis.producer

import java.io.File

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import com.amazonaws.services.kinesis.producer.{UserRecordFailedException, UserRecordResult}
import com.typesafe.config.ConfigFactory
import com.weightwatchers.reactive.kinesis.models.ProducerEvent
import com.weightwatchers.reactive.kinesis.producer.KinesisProducerActor.{
  Send,
  SendFailed,
  SendSuccessful,
  SendWithCallback
}
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FreeSpecLike, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

//scalastyle:off magic.number
class ProducerConfSpec
    extends TestKit(ActorSystem("producer-spec"))
    with ImplicitSender
    with FreeSpecLike
    with Matchers
    with MockitoSugar
    with BeforeAndAfterAll {

  val defaultKinesisConfig =
    ConfigFactory.parseFile(new File("src/main/resources/reference.conf")).getConfig("kinesis")

  val kinesisConfig = ConfigFactory
    .parseString("""
        |kinesis {
        |
        |   application-name = "TestSpec"
        |
        |   testProducer {
        |      stream-name = "core-test-kinesis-producer"
        |
        |      akka {
        |         max-outstanding-requests = 50000
        |      }
        |
        |      kpl {
        |         Region = us-east-1
        |         KinesisEndpoint = "CustomKinesisEndpoint"
        |         KinesisPort = 1111
        |         CredentialsRefreshDelay = 5001
        |         CloudwatchEndpoint = "CustomCloudWatchEndpoint"
        |         CloudwatchPort = 2222
        |         EnableCoreDumps = true
        |         NativeExecutable = "NativeExecutable"
        |         TempDirectory = "TempDirectory"
        |         ThreadPoolSize = 1
        |         ThreadingModel = "ThreadingModel.POOLED"
        |      }
        |   }
        |}
      """.stripMargin)
    .getConfig("kinesis")
    .withFallback(defaultKinesisConfig)

  implicit val timeout = Timeout(5.seconds)

  import system.dispatcher

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, timeout.duration)
  }

  "The KinesisProducerActor" - {

    "Should parse the Config into a ProducerConf" in {
      val producerConf = ProducerConf(kinesisConfig, "testProducer")

      producerConf.dispatcher should be(Some("kinesis.akka.default-dispatcher"))
      producerConf.kplConfig.getString("Region") should be("us-east-1")                            //validate an override properly
      producerConf.kplConfig.getBoolean("AggregationEnabled") should be(true)                      //validate a default property
      producerConf.kplConfig.getString("KinesisEndpoint") should be("CustomKinesisEndpoint")       //validate an override property
      producerConf.kplConfig.getLong("KinesisPort") should be(1111)                                //validate an override property
      producerConf.kplConfig.getLong("CredentialsRefreshDelay") should be(5001)                    //validate an override property
      producerConf.kplConfig.getString("CloudwatchEndpoint") should be("CustomCloudWatchEndpoint") //validate an override property
      producerConf.kplConfig.getLong("CloudwatchPort") should be(2222)                             //validate an override property
      producerConf.kplConfig.getBoolean("EnableCoreDumps") should be(true)                         //validate an override property
      producerConf.kplConfig.getString("NativeExecutable") should be("NativeExecutable")           //validate an override property
      producerConf.kplConfig.getString("TempDirectory") should be("TempDirectory")                 //validate an override property
      producerConf.kplConfig.getString("ThreadingModel") should be("ThreadingModel.POOLED")        //validate an override property
      producerConf.kplConfig.getInt("ThreadPoolSize") should be(1)                                 //validate an override property
      producerConf.throttlingConf.get.maxOutstandingRequests should be(50000)
      producerConf.throttlingConf.get.retryDuration should be(100.millis)
      producerConf.streamName should be("core-test-kinesis-producer")
    }

  }
}

//scalastyle:on
