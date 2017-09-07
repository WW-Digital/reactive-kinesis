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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Date

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestActor.AutoPilot
import akka.testkit.{ImplicitSender, TestActor, TestDuration, TestKit, TestProbe}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.amazonaws.services.kinesis.clientlibrary.types.{
  InitializationInput,
  ProcessRecordsInput,
  UserRecord
}
import com.amazonaws.services.kinesis.model.Record
import com.weightwatchers.reactive.kinesis.consumer.ConsumerWorker.{
  ProcessEvents,
  ProcessingComplete
}
import com.weightwatchers.reactive.kinesis.models.{CompoundSequenceNumber, ConsumerEvent}
import org.joda.time.{DateTime, DateTimeZone}
import org.mockito.Mockito
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FreeSpecLike, Matchers}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{DurationDouble, FiniteDuration}
import scala.concurrent.{Await, Future, Promise}

class ConsumerProcessingManagerSpec
    extends TestKit(ActorSystem("checkpoint-worker-spec"))
    with ImplicitSender
    with FreeSpecLike
    with Matchers
    with MockitoSugar
    with BeforeAndAfterAll
    with ScalaFutures {

  val batchTimeout: FiniteDuration = 2.seconds.dilated

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(3, Seconds), interval = Span(50, Millis)) // scalastyle:off

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, 5.seconds)
  }

  "The ConsumerProcessingManager" - {
    "Should set the shardId on init" in {
      val worker  = TestProbe()
      val kcl     = mock[Worker]
      val shardId = "12345"

      val manager = new ConsumerProcessingManager(worker.ref, kcl, batchTimeout)
      manager.initialize(new InitializationInput().withShardId(shardId))

      manager.kinesisShardId should be(shardId)
    }

    "Given we send a message to the worker, handling the response" - {

      /**
        * The majority of the processing test is identical, it's not until completing the promise that they differ
        */
      trait ProcessingSetup {
        val workerResponse = Promise[ProcessingComplete]()
        //use a promise to block the response
        val worker = TestProbe()
        worker.setAutoPilot(workerAutoPilot(workerResponse))

        val kcl     = mock[Worker]
        val shardId = "12345"

        val checkpointer = mock[IRecordProcessorCheckpointer]

        val record1              = buildRecord("1", "payload1", new Date())
        val record2              = buildRecord("2", "payload2", new Date())
        val record3              = buildRecord("3", "payload3", new Date())
        val record4              = buildAggregatedRecord("4", 1, "payload3", new Date())
        val record5              = buildAggregatedRecord("4", 2, "payload3", new Date())
        val record6              = buildAggregatedRecord("4", 3, "payload3", new Date())
        val records: Seq[Record] = Seq(record1, record2, record3, record4, record5, record6)

        val processInput = new ProcessRecordsInput()
          .withCheckpointer(checkpointer)
          .withRecords(records.asJava)

        val manager = new ConsumerProcessingManager(worker.ref, kcl, batchTimeout)
        //to set the shardId
        manager.initialize(new InitializationInput().withShardId(shardId))

        //Capture the response in a future so we can verify it's completion only happens when we're done processing in the worker
        val processResult = Future {
          manager.processRecords(processInput)
        }

        //validate the probe received the Seq of ConsumerEvents
        val expectedMsg = ProcessEvents(
          ArrayBuffer(toConsumerEvent(record1),
                      toConsumerEvent(record2),
                      toConsumerEvent(record3),
                      toConsumerEvent(record4),
                      toConsumerEvent(record5),
                      toConsumerEvent(record6)),
          checkpointer,
          shardId
        )
        worker.expectMsg(batchTimeout, "Worker Expecting ProcessRecords", expectedMsg)

        processResult.isCompleted should be(false) //should be false until we complete the promise
      }

      "When the response is a success it should continue processing" in new ProcessingSetup {

        workerResponse.success(ProcessingComplete(true)) //complete with a success

        whenReady(processResult) { _ =>
          processResult.isCompleted should be(true)

          Mockito
            .verify(kcl, Mockito.never())
            .shutdown() //this proves we handled the result as a success not a failure

          manager.shuttingDown.get() should be(false)
        }

      }

      "When the response is a failed batch it should shutdown and stop processing" in new ProcessingSetup {

        whenReady(processResult) { _ =>
          workerResponse.success(ProcessingComplete(false)) //complete with a failed batch

          processResult.isCompleted should be(true)

          manager.shuttingDown.get() should be(true)

          Mockito
            .verify(kcl, Mockito.times(1))
            .startGracefulShutdown() //we failed so shutdown should have been called
        }
      }

      "When the response is a failed (exception) future it should shutdown and stop processing" in new ProcessingSetup {

        val ex = new Exception("TEST")
        ex.setStackTrace(Array.empty[StackTraceElement])

        workerResponse.failure(ex) //complete with an exception

        whenReady(processResult) { _ =>
          processResult.isCompleted should be(true)

          manager.shuttingDown.get() should be(true)

          Mockito
            .verify(kcl, Mockito.times(1))
            .startGracefulShutdown() //we failed so shutdown should have been called
        }
      }
    }

    "Should skip batches when shutting down" - {

      //use a promise to block the response
      val worker = TestProbe()

      val kcl     = mock[Worker]
      val shardId = "12345"

      val checkpointer         = mock[IRecordProcessorCheckpointer]
      val records: Seq[Record] = Seq(buildRecord("1", "payload1", new Date()))
      val processInput = new ProcessRecordsInput()
        .withCheckpointer(checkpointer)
        .withRecords(records.asJava)

      val manager = new ConsumerProcessingManager(worker.ref, kcl, batchTimeout)
      manager.shuttingDown.getAndSet(true)
      manager.initialize(new InitializationInput().withShardId(shardId))
      val processResult = Future {
        manager.processRecords(processInput)
      }

      //validate the probe received nothing
      worker.expectNoMsg(1.second)

      processResult.isCompleted should be(true) //should be false until we complete the promise
    }

  }

  /**
    * Acks all [[ProcessEvents]] messages with a corresponding [[ProcessingComplete]]
    *
    * @param response The response from this actor, a promise that we can control the completion of.
    */
  private def workerAutoPilot(response: Promise[ProcessingComplete]): AutoPilot = {
    import akka.pattern.pipe
    new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): AutoPilot =
        msg match {
          case ProcessEvents(_, _, _) =>
            response.future.pipeTo(sender)
            TestActor.KeepRunning
          case m => fail(s"Worker received unexpected message from manager $m")
        }
    }
  }

  def buildRecord(seqNo: String, payload: String, date: Date): UserRecord = {
    new UserRecord(
      new Record()
        .withSequenceNumber(seqNo)
        .withData(ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8)))
        .withApproximateArrivalTimestamp(date)
    )
  }

  def buildAggregatedRecord(seqNo: String,
                            subSeqNo: Long,
                            payload: String,
                            date: Date): UserRecord = {
    val userRecord = mock[UserRecord]
    Mockito.when(userRecord.getSequenceNumber).thenReturn(seqNo)
    Mockito.when(userRecord.getSubSequenceNumber).thenReturn(subSeqNo)
    Mockito.when(userRecord.getApproximateArrivalTimestamp).thenReturn(date)
    Mockito
      .when(userRecord.getData)
      .thenReturn(ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8)))
    userRecord
  }

  def toConsumerEvent(record: UserRecord): ConsumerEvent = {
    ConsumerEvent(
      CompoundSequenceNumber(record.getSequenceNumber, record.getSubSequenceNumber),
      new String(record.getData.array(), java.nio.charset.StandardCharsets.UTF_8),
      new DateTime(record.getApproximateArrivalTimestamp, DateTimeZone.UTC)
    )
  }

}
