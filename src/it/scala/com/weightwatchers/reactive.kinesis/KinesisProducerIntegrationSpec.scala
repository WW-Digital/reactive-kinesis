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

package com.weightwatchers.reactive.kinesis

import java.io.File

import com.amazonaws.services.kinesis.producer.{KinesisProducer => AWSKinesisProducer}
import com.typesafe.config.ConfigFactory
import com.weightwatchers.reactive.kinesis.common.{KinesisTestConsumer, TestCredentials}
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer.ConsumerConf
import com.weightwatchers.reactive.kinesis.models.ProducerEvent
import com.weightwatchers.reactive.kinesis.producer.{KinesisProducer, ProducerConf}
import org.scalatest.concurrent.Eventually
import org.scalatest.mockito.MockitoSugar
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FreeSpec, Matchers}

import scala.concurrent.duration._
import scala.util.Random

//scalastyle:off magic.number
class KinesisProducerIntegrationSpec
  extends FreeSpec
    with Matchers
    with MockitoSugar
    with BeforeAndAfterAll
    with Eventually {

  implicit val ece = scala.concurrent.ExecutionContext.global

  val defaultKinesisConfig =
    ConfigFactory.parseFile(new File("src/main/resources/reference.conf")).getConfig("kinesis")

  val kinesisConfig = ConfigFactory
    .parseString(
      """
        |kinesis {
        |
        |   application-name = "ScalaProducerTestSpec"
        |
        |   testProducer {
        |      stream-name = "int-test-stream-1"
        |
        |      kpl {
        |         Region = us-east-1
        |
        |         CloudwatchEndpoint = localhost
        |         CloudwatchPort = 4582
        |
        |         KinesisEndpoint = localhost
        |         KinesisPort = 4568
        |
        |         VerifyCertificate = false
        |      }
        |   }
        |
        |   testConsumer {
        |      stream-name = "int-test-stream-1"
        |
        |      kcl {
        |         AWSCredentialsProvider = "com.weightwatchers.reactive.kinesis.common.TestCredentials|foo|bar"
        |         regionName = us-east-1
        |         KinesisEndpoint = "https://localhost:4568"
        |         DynamoDBEndpoint = "https://localhost:4569"
        |
        |
        |         metricsLevel = None
        |      }
        |
        |   }
        |}
      """.stripMargin
    )
    .getConfig("kinesis")
    .withFallback(defaultKinesisConfig)


  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(100, Millis))

  val consumer: KinesisTestConsumer = KinesisTestConsumer.from(ConsumerConf(kinesisConfig, "testConsumer"), Some(100 millis))

  override protected def afterAll(): Unit = {
    consumer.shutdown
  }

  "The KinesisProducer" - {

    "Should publish a message to a stream" in {

      val producerConf = ProducerConf(kinesisConfig, "testProducer", Some(TestCredentials.Credentials))
      val producer = KinesisProducer(producerConf)

      val existingRecordCount = consumer.retrieveRecords(producerConf.streamName, 10).size

      val event = ProducerEvent("1234", Random.alphanumeric.take(10).mkString)
      producer.addUserRecord(event)

      eventually {
        val records: Seq[String] = consumer.retrieveRecords(producerConf.streamName, 10)
        records.size shouldBe (existingRecordCount + 1)
        records should contain(new String(event.payload.array(), java.nio.charset.StandardCharsets.UTF_8))
      }
    }
  }
}

//scalastyle:on
