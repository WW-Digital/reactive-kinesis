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

import com.amazonaws.services.kinesis.producer.{
  UserRecordResult,
  KinesisProducer => AWSKinesisProducer
}
import com.google.common.util.concurrent.SettableFuture
import com.typesafe.config.ConfigFactory
import com.weightwatchers.reactive.kinesis.models.ProducerEvent
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FreeSpec, Matchers}

import scala.concurrent.Future

//scalastyle:off magic.number
class KinesisProducerSpec extends FreeSpec with Matchers with MockitoSugar with BeforeAndAfterAll {

  implicit val ece = scala.concurrent.ExecutionContext.global

  val defaultKinesisConfig =
    ConfigFactory.parseFile(new File("src/main/resources/reference.conf")).getConfig("kinesis")

  val kinesisConfig = ConfigFactory
    .parseString(
      """
        |kinesis {
        |
        |   application-name = "TestSpec"
        |
        |   testProducer {
        |      # The name of the producer stream
        |      stream-name = "core-test-kinesis-producer"
        |
        |      # Can specify settings here as per default-producer, to override those defaults for this producer.
        |
        |      kpl {
        |         Region = us-east-1
        |      }
        |   }
        |}
      """.stripMargin
    )
    .getConfig("kinesis")
    .withFallback(defaultKinesisConfig)

  "The KinesisProducer" - {

    "Should create the KinesisProducerKPL with an underlying AWSKinesisProducer" in {

      val producer = KinesisProducer(ProducerConf(kinesisConfig, "testProducer"))

      producer.underlying should not be (null) // scalastyle:ignore

      producer.underlying.destroy()
    }

    "Should Add a Record to the Kinesis Stream, wrapping the response in a scala future" in {

      val streamName = kinesisConfig.getString("testProducer.stream-name")

      val awsProducer   = mock[AWSKinesisProducer]
      val scalaProducer = new KinesisProducer(awsProducer, streamName)

      val result = mock[UserRecordResult]
      val event  = ProducerEvent("111", "das payload")

      val f: SettableFuture[UserRecordResult] = SettableFuture.create()
      f.set(result)

      //Given a successful response from the underlying producer
      when(result.getShardId).thenReturn("SUCCESS!!")
      when(awsProducer.addUserRecord(streamName, event.partitionKey, event.payload)).thenReturn(f)

      //When we send an event
      val future: Future[UserRecordResult] = scalaProducer.addUserRecord(event)

      //Then we receive a Scala future wrapping the result
      ScalaFutures.whenReady(future) { f =>
        f.getShardId should equal("SUCCESS!!")
      }

      //Then we should receive a SendSuccessful response
      verify(awsProducer, times(1)).addUserRecord(streamName, event.partitionKey, event.payload)
    }

    "Should keep track of the outstanding requests" in {

      val streamName = kinesisConfig.getString("testProducer.stream-name")

      val awsProducer   = mock[AWSKinesisProducer]
      val scalaProducer = new KinesisProducer(awsProducer, streamName)

      //Given a 5 requests in progress
      when(awsProducer.getOutstandingRecordsCount()).thenReturn(5)

      //Then we should receive 5 when invoking outstandingRecordsCount
      scalaProducer.outstandingRecordsCount() should equal(5)
    }

    "Should gracefully stop the underlying producer" in {

      val streamName = kinesisConfig.getString("testProducer.stream-name")

      val awsProducer   = mock[AWSKinesisProducer]
      val scalaProducer = new KinesisProducer(awsProducer, streamName)

      //Given we call stop
      scalaProducer.stop()

      //Then we should flush and destroy the underlying producer
      verify(awsProducer, times(1)).flushSync()
      verify(awsProducer, times(1)).destroy()
      scalaProducer.destroyed() should equal(true)
    }
  }
}

//scalastyle:on
