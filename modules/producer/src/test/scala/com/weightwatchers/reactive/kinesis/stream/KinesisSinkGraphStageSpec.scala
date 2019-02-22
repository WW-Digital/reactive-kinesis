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

package com.weightwatchers.reactive.kinesis.stream

import java.util.Collections

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.{TestActorRef, TestKit}
import com.amazonaws.services.kinesis.producer.UserRecordResult
import com.weightwatchers.reactive.kinesis.models.ProducerEvent
import com.weightwatchers.reactive.kinesis.producer.KinesisProducerActor.{
  SendFailed,
  SendSuccessful,
  SendWithCallback
}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FreeSpecLike, Matchers}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class KinesisSinkGraphStageSpec
    extends TestKit(ActorSystem("source-graph-spec"))
    with FreeSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with Eventually {

  implicit val materializer: Materializer = ActorMaterializer()
  implicit val ec                         = system.dispatcher
  implicit val defaultPatience            = PatienceConfig(5.seconds, interval = 50.millis)

  "KinesisSinkGraph" - {

    "all messages are send to the producer, while the stream is finished only, if all messages got acknowledged" in sinkWithProducer(
      ackMessage
    ) { (sink, producer) =>
      val messages = 1.to(100).map(_.toString).map(num => ProducerEvent(num, num))
      Source(messages).runWith(sink).futureValue
      producer.underlyingActor.allMessages should have size 100
      producer.underlyingActor.allMessages.values should contain allElementsOf messages
    }

    "the stream fails, if the producer can not send" in sinkWithProducer(failMessage) { (sink, _) =>
      val messages = 1.to(100).map(_.toString).map(num => ProducerEvent(num, num))
      val result   = Source(messages).runWith(sink).failed.futureValue
      result shouldBe a[IllegalStateException]
    }

    "the stream fails, if upstream fails" in sinkWithProducer(ackMessage) { (sink, _) =>
      val exception = new IllegalStateException("boom")
      val result    = Source.failed(exception).runWith(sink).failed.futureValue
      result shouldBe exception
    }

    "the stream fails, if the producer actor dies" in {
      val sink     = ProducerStreamFactory.sink(Props(new FailActor), 1)
      val messages = 1.to(100).map(_.toString).map(num => ProducerEvent(num, num))
      val result   = Source(messages).runWith(sink).failed.futureValue
      result shouldBe a[IllegalStateException]
    }

    "do not send more messages than maxOutstanding" in sinkWithProducer(
      ignoreAndFailOn(_.partitionKey.toInt > 5)
    ) { (sink, producer) =>
      val messages = Source(1.to(100).map(_.toString).map(num => ProducerEvent(num, num)))
      val result   = messages.runWith(sink)
      eventually { producer.underlyingActor.allMessages should have size 5 }
      // the stream would fail if we would read the 6th. element
      result.isCompleted shouldBe false
      producer.underlyingActor.allMessages should have size 5
    }

    "A sink can be created from system config" in {
      ProducerStreamFactory.sink("test-producer")
    }
  }

  class ForwardToProducerActor(ref: ActorRef) extends Actor {
    override def receive: Receive = {
      case message => ref.forward(message)
    }
  }

  class TestProducerActor(sendFn: (ActorRef, SendWithCallback) => Unit) extends Actor {
    val allMessages = mutable.AnyRefMap.empty[String, ProducerEvent]
    override def receive: Receive = {
      case send: SendWithCallback =>
        allMessages += send.messageId -> send.producerEvent
        sendFn(sender(), send)
    }
  }

  class FailActor extends Actor {
    override def receive: Receive = throw new IllegalStateException("wrong!")
  }

  def ignoreAndFailOn(decider: ProducerEvent => Boolean)(sender: ActorRef,
                                                         event: SendWithCallback): Unit = {
    if (decider(event.producerEvent)) failMessage(sender, event) else ignoreMessage(sender, event)
  }

  def ackMessage(sender: ActorRef, event: SendWithCallback): Unit = {
    val recordResult = new UserRecordResult(Collections.emptyList(), "123", "shard", true)
    sender ! SendSuccessful(event.messageId, recordResult)
  }

  def failMessage(sender: ActorRef, event: SendWithCallback): Unit = {
    sender ! SendFailed(event.messageId, new IllegalStateException("wrong!"))
  }

  def ignoreMessage(sender: ActorRef, event: SendWithCallback): Unit = ()

  def sinkWithProducer(sendFn: (ActorRef, SendWithCallback) => Unit, maxOutstanding: Int = 5)(
      sinkFn: (Sink[ProducerEvent, Future[Done]], TestActorRef[TestProducerActor]) => Unit
  ) = {
    val testActor = TestActorRef[TestProducerActor](Props(new TestProducerActor(sendFn)))
    val sink =
      ProducerStreamFactory.sink(Props(new ForwardToProducerActor(testActor)), maxOutstanding)
    sinkFn(sink, testActor)
  }

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, 5.seconds)
  }
}
