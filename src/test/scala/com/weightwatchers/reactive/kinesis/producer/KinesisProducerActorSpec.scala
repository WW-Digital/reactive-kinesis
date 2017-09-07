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

import akka.actor.{ActorSystem, PoisonPill}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import com.amazonaws.services.kinesis.producer.{UserRecordFailedException, UserRecordResult}
import com.weightwatchers.reactive.kinesis.models.ProducerEvent
import com.weightwatchers.reactive.kinesis.producer.KinesisProducerActor._
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FreeSpecLike, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

//scalastyle:off magic.number
class KinesisProducerActorSpec
  extends TestKit(ActorSystem("producer-spec"))
    with ImplicitSender
    with FreeSpecLike
    with Matchers
    with MockitoSugar
    with BeforeAndAfterAll {

  implicit val timeout = Timeout(5.seconds)

  import system.dispatcher

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, timeout.duration)
  }

  "The KinesisProducerActor" - {

    "Should process a message without a response for a Send" in {
      val probe = TestProbe()

      val event = ProducerEvent("111", "das payload")
      val result = mock[UserRecordResult]
      val producer = mock[KinesisProducerKPL]

      //Given a successful response from the underlying producer
      when(producer.addUserRecord(event)).thenReturn(Future {
        result
      })
      when(producer.outstandingRecordsCount()).thenReturn(0)

      //When we send a Send
      val producerActor = system.actorOf(KinesisProducerActor.props(producer))
      producerActor.tell(Send(event), probe.ref)

      //Then we should receive a SendSuccessful response
      probe.expectNoMsg(500.millis)
      verify(producer, times(1)).addUserRecord(event)
    }

    "Should process a message and respond with SendSuccessful upon completion for SendWithCallback" in {
      val probe = TestProbe()

      val event = ProducerEvent("111", "das payload")
      val result = mock[UserRecordResult]
      val producer = mock[KinesisProducerKPL]

      //Given a successful response from the underlying producer
      when(producer.addUserRecord(event)).thenReturn(Future {
        result
      })
      when(producer.outstandingRecordsCount()).thenReturn(0)

      //When we send a SendWithCallback
      val producerActor = system.actorOf(KinesisProducerActor.props(producer))
      val msg = SendWithCallback(event)
      producerActor.tell(msg, probe.ref)

      //Then we should receive a SendSuccessful response
      probe.expectMsg(SendSuccessful(msg.messageId, result))
      verify(producer, times(1)).addUserRecord(event)
    }

    "Should process a message and respond with SendFailed upon failure with SendWithCallback" in {
      val probe = TestProbe()

      val event = ProducerEvent("111", "das payload")
      val result = mock[UserRecordResult]
      val ex = new UserRecordFailedException(result)

      //Given an exception from the underlying producer
      val producer = mock[KinesisProducerKPL]
      when(producer.addUserRecord(event)).thenReturn(Future {
        throw ex
      })
      when(producer.outstandingRecordsCount()).thenReturn(0)

      //When we send a SendWithCallback
      val producerActor = system.actorOf(KinesisProducerActor.props(producer))
      val msg = SendWithCallback(event)
      producerActor.tell(msg, probe.ref)

      //Then we should receive a SendSuccessful response
      probe.expectMsg(SendFailed(msg.messageId, ex))
      verify(producer, times(1)).addUserRecord(event)
    }

    "Should throttle when over the configured maxOutstandingRequests" in {
      val probe = TestProbe()

      val maxOutstandingRequests = 10
      val actualOutstandingRequests = 10
      val event = ProducerEvent("111", "das payload")
      val result = mock[UserRecordResult]

      //Given that the number of requests in progress >= maxOutstandingRequests
      val producer = mock[KinesisProducerKPL]
      when(producer.addUserRecord(event)).thenReturn(Future {
        result
      })
      when(producer.outstandingRecordsCount()).thenReturn(actualOutstandingRequests)

      //When we send a SendWithCallback
      val producerActor =
        system.actorOf(KinesisProducerActor.props(producer, maxOutstandingRequests))
      val msg = SendWithCallback(event)
      producerActor.tell(msg, probe.ref)

      //Then we receive and process no messages
      probe.expectNoMsg(500.millis)
      verify(producer, times(0)).addUserRecord(event)

      //Until we have LESS than 9 requests in progress (DEFAULT is 10% less than max to unthrottle)
      when(producer.outstandingRecordsCount()).thenReturn(9)

      //Still not LESS than 9
      probe.expectNoMsg(500.millis)
      verify(producer, times(0)).addUserRecord(event)

      when(producer.outstandingRecordsCount()).thenReturn(8)

      //Then we should receive a SendSuccessful response
      probe.expectMsg(SendSuccessful(msg.messageId, result))
      verify(producer).addUserRecord(event)
    }

    "Should gracefully shutdown the underlying producer" in {
      val producer = mock[KinesisProducerKPL]
      when(producer.outstandingRecordsCount()).thenReturn(0)

      //When we send a SendWithCallback
      val producerActor = TestActorRef(KinesisProducerActor.props(producer))

      producerActor ! PoisonPill

      verify(producer, times(1)).stop()
    }

  }
}

//scalastyle:on
