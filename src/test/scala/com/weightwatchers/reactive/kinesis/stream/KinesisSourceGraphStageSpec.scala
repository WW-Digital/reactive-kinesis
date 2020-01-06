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

import akka.actor.ActorRef
import akka.actor.Status.Failure
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{BufferOverflowException, ThrottleMode}
import akka.testkit.TestProbe
import akka.{Done, NotUsed}
import com.typesafe.config.ConfigFactory
import com.weightwatchers.reactive.kinesis.consumer.ConsumerService
import com.weightwatchers.reactive.kinesis.consumer.ConsumerWorker.{
  ConsumerWorkerFailure,
  ProcessEvent
}
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer.ConsumerConf
import com.weightwatchers.reactive.kinesis.models.{CompoundSequenceNumber, ConsumerEvent}
import com.weightwatchers.reactive.kinesis.{AkkaTest, UnitTest}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Date
import org.joda.time.DateTime
import org.scalatest.time.{Millis, Seconds, Span}
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

class KinesisSourceGraphStageSpec extends UnitTest with AkkaTest {

  implicit val ec = system.dispatcher
  implicit val defaultPatience =
    PatienceConfig(timeout = Span(3, Seconds), interval = Span(50, Millis))
  // create implicit sender
  implicit val self = TestProbe().ref

  "KinesisSourceGraph" - {

    "process events correctly" in new Fixture {
      val source = withStageActor { ref =>
        ref ! processEvent("1")
        ref ! processEvent("2")
        ref ! processEvent("3")
        ref ! processEvent("4")
      }
      val all = source.take(4).runWith(Sink.seq).futureValue
      all should have size 4
      all.map(_.payload.payloadAsString()) shouldBe Seq("1", "2", "3", "4") //correct order is maintained
    }

    "allows mapped and async mapped stage events" in new Fixture {
      val source = withStageActor { ref =>
        ref ! processEvent("123")
        ref ! processEvent("312")
      }
      val all = source
        .map(_.map(_.payloadAsString().toLong)) // CommittableEvent ConsumerEvent => Long
        .mapAsync(1)(_.mapAsync(long => Future(new Date(long)))) // CommittableEvent Long => Date
        .take(2)
        .runWith(Sink.seq)
        .futureValue
      all should have size 2
      all.foreach(_.payload shouldBe a[Date])
    }

    "source is stopped, if the consumer service finishes" in new Fixture {
      val source = withStageActor { ref =>
        ref ! processEvent("Test")
        // signal service has been finished
        ref ! Done
      }
      val all = source.runWith(Sink.seq).futureValue
      all should have size 1
    }

    "source is stopped, if the consumer service fails" in new Fixture {
      val source = withStageActor { ref =>
        // signal a service failure
        ref ! Failure(new IllegalStateException("boom!"))
      }
      val all = source.runWith(Sink.seq).failed.futureValue
      all shouldBe a[IllegalStateException]
    }

    "source is stopped, if events do not get processed" in new Fixture {
      val source = withStageActor { ref =>
        // signal a consumer worker failure
        ref ! ConsumerWorkerFailure(Seq.empty, "test")
      }
      val all = source.runWith(Sink.seq).failed.futureValue
      all shouldBe a[IllegalStateException]
    }

    "source is stopped, if the internal buffer is exceeded" in new Fixture {
      val source = withStageActor { ref =>
        0.until(10).foreach(num => ref ! processEvent(num.toString))
      }
      val all =
        source.throttle(1, 1.hour, 0, ThrottleMode.Shaping).runWith(Sink.seq).failed.futureValue
      all shouldBe a[BufferOverflowException]
    }
  }

  class TestConsumerService(actorFn: ActorRef => Unit)(actorRef: ActorRef) extends ConsumerService {
    val finished: Promise[Unit] = Promise[Unit]()
    override def stop(): Unit   = ()
    override def start(): Future[Unit] = {
      actorFn(actorRef)
      finished.future
    }
  }

  class Fixture {
    val kinesisConfig = ConfigFactory
      .parseString("""
         |kinesis {
         |  application-name: "KinesisSourceGraphSpec"
         |  testConsumer {
         |    stream-name = "foo"
         |    kcl {
         |      maxRecords = 1
         |    }
         |  }
         |}
       """.stripMargin)
      .withFallback(ConfigFactory.load())
      .getConfig("kinesis")

    val consumerConf = ConsumerConf(kinesisConfig, "testConsumer")

    def withStageActor(
        action: ActorRef => Unit
    ): Source[CommittableEvent[ConsumerEvent], NotUsed] = {
      Source.fromGraph(
        new KinesisSourceGraphStage(consumerConf, new TestConsumerService(action)(_), system)
      )
    }

    val seqNr = 0.until(Int.MaxValue).iterator.map(_.toLong)
    def processEvent(payload: String): ProcessEvent =
      ProcessEvent(
        ConsumerEvent(CompoundSequenceNumber("fixed", seqNr.next()),
                      ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8)),
                      DateTime.now())
      )
  }
}
