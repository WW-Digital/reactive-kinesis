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

import com.weightwatchers.reactive.kinesis.models.ProducerEvent
import com.weightwatchers.reactive.kinesis.producer.KinesisProducer
import org.scalatest.concurrent.Eventually
import org.scalatest.mockito.MockitoSugar
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FreeSpec, Matchers}

import scala.language.postfixOps
import scala.util.Random

//scalastyle:off magic.number
class KinesisProducerIntegrationSpec
    extends FreeSpec
    with Matchers
    with MockitoSugar
    with BeforeAndAfterAll
    with Eventually
    with KinesisSuite {

  implicit val ece = scala.concurrent.ExecutionContext.global

  val TestStreamNrOfMessagesPerShard: Long = 0

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(100, Millis))

  "The KinesisProducer" - {

    "Should publish a message to a stream" in new withKinesisConfForApp(
      "int-test-stream-producer-1"
    ) {

      val conf     = producerConf()
      val producer = KinesisProducer(conf)

      val existingRecordCount = testConsumer.retrieveRecords(conf.streamName, 10).size

      val event = ProducerEvent("1234", Random.alphanumeric.take(10).mkString)
      producer.addUserRecord(event)

      eventually {
        val records: Seq[String] = testConsumer.retrieveRecords(conf.streamName, 10)
        records.size shouldBe (existingRecordCount + 1)
        records should contain(
          new String(event.payload.array(), java.nio.charset.StandardCharsets.UTF_8)
        )
      }
    }
  }
}

//scalastyle:on
