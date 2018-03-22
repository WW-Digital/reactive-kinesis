/*
 * Copyright 2017 WeightWatchers
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.weightwatchers.reactive.kinesis.consumer

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.testkit.{TestActorRef, TestDuration, TestKit, TestProbe}
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.typesafe.config.ConfigFactory
import com.weightwatchers.reactive.kinesis.consumer.CheckpointWorker._
import com.weightwatchers.reactive.kinesis.models.CompoundSequenceNumber
import com.weightwatchers.reactive.kinesis.consumer.CheckpointWorker.ReadyToCheckpoint
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FreeSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

//scalastyle:off magic.number
class CheckpointWorkerSpec
    extends TestKit(ActorSystem("checkpoint-worker-spec"))
    with FreeSpecLike
    with Matchers
    with MockitoSugar
    with BeforeAndAfterAll {

  val config = ConfigFactory.parseString("""
      |kinesis {
      |  application-name: "TestCoreEventingHappyMagic"
      |  aws {
      |    profile: "ww"
      |  }
      |  testConsumer {
      |    stream-name = "core-test-bar"
      |
      |    worker {
      |      batchTimeoutSeconds = 3
      |    }
      |
      |    kcl {
      |      initialPosition = "LATEST"
      |    }
      |
      |    checkpointer {
      |      backoffMillis = 3000
      |      intervalMillis = 2000
      |      notificationDelayMillis = 1000
      |    }
      |  }
      |}
    """.stripMargin).getConfig("kinesis")

  val parsedCheckpointerConf = CheckpointerConf(config.getConfig("testConsumer"))
  //Created a dilated config so we can scale timeouts for slow envs (this spec is time sensitive)
  val checkpointerConf = CheckpointerConf(parsedCheckpointerConf.backoff.dilated,
                                          parsedCheckpointerConf.interval.dilated,
                                          parsedCheckpointerConf.notificationDelay.dilated)

  val readyToCheckpointTimeout: FiniteDuration      = 2.seconds
  val emptyCheckpointMailboxTimeout: FiniteDuration = 250.millis
  val checkpointResultTimeout: FiniteDuration       = 2000.millis
  val checkpointNotificationTimeout: FiniteDuration = 1500.millis
  val checkpointBackoffTimeout: FiniteDuration      = parsedCheckpointerConf.backoff.dilated

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, 5.seconds)
  }

  "The CheckpointWorker" - {

    "Should notify when ready to checkpoint" in {
      val probe = TestProbe()

      createCheckpointWorkerParentForwarder(probe)

      probe.expectMsg(readyToCheckpointTimeout,
                      "Initial Checkpoint Notification",
                      ReadyToCheckpoint)
      probe.expectMsg(checkpointNotificationTimeout,
                      "Subsequent Checkpoint Notification",
                      ReadyToCheckpoint)
    }

    "Should checkpoint when ready, cancelling any notifications until the next interval" in {
      val probe = TestProbe()

      val worker = createCheckpointWorkerParentForwarder(probe)

      val checkpointer   = mock[IRecordProcessorCheckpointer]
      val sequenceNumber = CompoundSequenceNumber("10", 1)

      //When we're told to checkpoint
      probe.expectMsg(readyToCheckpointTimeout,
                      "Initial Checkpoint Notification",
                      ReadyToCheckpoint)

      //Then send a checkpoint message
      probe.send(worker, Checkpoint(checkpointer, sequenceNumber))

      //Empty the mailbox of any ReadyToCheckpoint messages
      probe.receiveWhile(emptyCheckpointMailboxTimeout, emptyCheckpointMailboxTimeout, 5) {
        case ReadyToCheckpoint =>
      }

      //So that we can cleanly await ONLY our CheckpointResult
      probe.expectMsg(checkpointResultTimeout,
                      "Expecting Only Successful CheckpointResult",
                      CheckpointResult(sequenceNumber, success = true))

      //Then verify the checkpointer was used
      Mockito
        .verify(checkpointer)
        .checkpoint(sequenceNumber.sequenceNumber, sequenceNumber.subSequenceNumber)

      //Finally assert that we don't receive another ReadyToCheckpoint message until the configured interval (2 seconds + 1 second)
      probe.expectNoMessage(readyToCheckpointTimeout)
      probe.expectMsg(checkpointNotificationTimeout,
                      "Subsequent Checkpoint Notification",
                      ReadyToCheckpoint)
    }

    "Should backoff when the checkpointer throws a ThrottlingException" in {
      val probe = TestProbe()

      val worker = createCheckpointWorkerParentForwarder(probe)

      val sequenceNumber = CompoundSequenceNumber("10", 1)
      val checkpointer   = mock[IRecordProcessorCheckpointer]
      Mockito
        .when(
          checkpointer.checkpoint(sequenceNumber.sequenceNumber, sequenceNumber.subSequenceNumber)
        )
        .thenThrow(new ThrottlingException("Dynamo says wait!!"))

      //When we're told to checkpoint
      probe.expectMsg(readyToCheckpointTimeout,
                      "Initial Checkpoint Notification",
                      ReadyToCheckpoint)

      //Then send a checkpoint message
      probe.send(worker, Checkpoint(checkpointer, sequenceNumber))

      //Empty the mailbox of any ReadyToCheckpoint messages
      probe.receiveWhile(emptyCheckpointMailboxTimeout, emptyCheckpointMailboxTimeout, 5) {
        case ReadyToCheckpoint =>
      }

      //So that we can cleanly await ONLY our CheckpointResult
      probe.expectMsg(checkpointResultTimeout,
                      "Expecting Only Failed CheckpointResult",
                      CheckpointResult(sequenceNumber, success = false))

      //Then verify the checkpointer was used
      Mockito
        .verify(checkpointer)
        .checkpoint(sequenceNumber.sequenceNumber, sequenceNumber.subSequenceNumber)

      //Finally assert that we don't receive another ReadyToCheckpoint message until the configured BACKOFF interval (3 seconds)
      probe.expectNoMessage(checkpointBackoffTimeout)
      probe.expectMsg(checkpointNotificationTimeout,
                      "Subsequent Checkpoint Notification after backoff",
                      ReadyToCheckpoint)
    }

    "Should return NotReady when Checkpoint received whilst not in Ready state" in {
      val probe = TestProbe()

      val worker = createCheckpointWorkerParentForwarder(probe)

      val sequenceNumber = CompoundSequenceNumber("10", 1)
      val checkpointer   = mock[IRecordProcessorCheckpointer]

      //When we're told to checkpoint
      probe.expectMsg(readyToCheckpointTimeout,
                      "Initial Checkpoint Notification",
                      ReadyToCheckpoint)

      //Then send a checkpoint message
      probe.send(worker, Checkpoint(checkpointer, sequenceNumber))

      //Empty the mailbox of any messages (Ready + CheckpointResult)
      probe.receiveWhile(emptyCheckpointMailboxTimeout, emptyCheckpointMailboxTimeout, 5) {
        case _ =>
      }

      //Then send another checkpoint message
      probe.send(worker, Checkpoint(checkpointer, sequenceNumber))

      //So that we can cleanly await ONLY our Not Ready
      probe.expectMsg(checkpointResultTimeout,
                      "Expecting NotReady",
                      NotReady(Checkpoint(checkpointer, sequenceNumber)))
    }

    "Should checkpoint when Checkpoint received whilst not in Ready state with force set to true" in {
      val probe = TestProbe()

      val worker = createCheckpointWorkerParentForwarder(probe)

      val sequenceNumber = CompoundSequenceNumber("10", 1)
      val checkpointer   = mock[IRecordProcessorCheckpointer]

      //When we're told to checkpoint
      probe.expectMsg(readyToCheckpointTimeout,
                      "Initial Checkpoint Notification",
                      ReadyToCheckpoint)

      //Then send a checkpoint message
      probe.send(worker, Checkpoint(checkpointer, sequenceNumber))

      //Empty the mailbox of any messages (Ready + CheckpointResult)
      probe.receiveWhile(emptyCheckpointMailboxTimeout, emptyCheckpointMailboxTimeout, 5) {
        case _ =>
      }

      //Then send another checkpoint message
      probe.send(worker, Checkpoint(checkpointer, sequenceNumber, force = true))

      //So that we can cleanly await ONLY our CheckpointResult
      probe.expectMsg(checkpointResultTimeout,
                      "Expecting Only Successful CheckpointResult",
                      CheckpointResult(sequenceNumber, success = true))

      //Finally assert that we don't receive another ReadyToCheckpoint message, ever
      probe.expectNoMessage(readyToCheckpointTimeout)
      probe.expectNoMessage(readyToCheckpointTimeout)
    }
  }

  /**
    * Creates an Actor which is the parent of the CheckpointWorker.
    * Any messages received by the parent are forwarded to the probe.
    * Returns the CheckpointWorker actor.
    * We need this as [[CheckpointWorker.ReadyToCheckpoint]] messages are sent directly to the parent (not the sender).
    */
  private def createCheckpointWorkerParentForwarder(probe: TestProbe): ActorRef = {
    val parent = TestActorRef(new Actor {

      val checkpointWorker =
        context.actorOf(CheckpointWorker.props(checkpointerConf), "checkpointWorker")

      def receive = {
        case x =>
          probe.ref forward x

      }
    })

    parent.underlying.child("checkpointWorker").get
  }
}

//scalastyle:on
