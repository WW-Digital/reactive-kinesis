/*
 Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.nio.ByteBuffer

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.TestActor.AutoPilot
import akka.testkit.{ImplicitSender, TestActor, TestDuration, TestKit, TestProbe}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.weightwatchers.reactive.kinesis.consumer.CheckpointWorker.{
  Checkpoint,
  CheckpointResult,
  CheckpointerConf,
  ReadyToCheckpoint
}
import com.weightwatchers.reactive.kinesis.consumer.ConsumerWorker._
import com.weightwatchers.reactive.kinesis.models.{CompoundSequenceNumber, ConsumerEvent}
import org.joda.time.DateTime
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FreeSpecLike, GivenWhenThen, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.{DurationInt, FiniteDuration}

class ConsumerWorkerSpec
    extends TestKit(ActorSystem("consumer-worker-spec"))
    with ImplicitSender
    with FreeSpecLike
    with Matchers
    with MockitoSugar
    with BeforeAndAfterAll
    with GivenWhenThen {

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, 5.seconds)
  }

  "The ConsumerWorker" - {

    "Given an event processor that acks everything except 911" - {
      trait Setup {
        val noAckSeqNo = CompoundSequenceNumber("911", 0)
        val noAckRecord =
          createEvent(noAckSeqNo.sequenceNumber, noAckSeqNo.subSequenceNumber, "payload911")
        lazy val noAckSeqNos: Seq[CompoundSequenceNumber]  = Seq(noAckSeqNo)
        lazy val failedSeqNos: Seq[CompoundSequenceNumber] = Nil
        lazy val batchRetries                              = 0
        lazy val failureTolerance                          = 0.0
        lazy val batchTimeout                              = 5.seconds.dilated
        lazy val shutdownTimeout                           = 100.millis.dilated

        val messageConfirmationTimeout: FiniteDuration = batchTimeout.minus(1.second).dilated
        val batchConfirmationTimeout: FiniteDuration =
          (batchTimeout * (batchRetries + 1).toLong).dilated
        val checkpointTimeout: FiniteDuration = 200.millis.dilated
        val noMsgTimeout: FiniteDuration      = 200.millis.dilated

        val managerProbe      = TestProbe()
        val checkpointerProbe = TestProbe()
        val processorProbe    = TestProbe()

        //Ack processing of each message
        processorProbe.setAutoPilot(processorSuccessfulAutoPilot(noAckSeqNos, failedSeqNos))

        val checkpointerConf = CheckpointerConf(batchTimeout, batchTimeout, batchTimeout)

        //create a worker which will forward messages to our checkpointer probe
        val worker: ActorRef = system.actorOf(
          Props(
            classOf[ConsumerWorker],
            processorProbe.ref,
            ConsumerWorkerConf(batchTimeout,
                               batchRetries,
                               failureTolerance,
                               false,
                               shutdownTimeout),
            Props(new ChildForwarder(checkpointerProbe.ref))
          )
        )
      }

      "Should successfully process a batch, notifying the Manager upon completion" in new Setup {

        val event1 = createEvent("1", 1, "payload1")
        val event2 = createEvent("1", 2, "payload2")
        val event3 = createEvent("3", 1, "payload3")
        val event4 = createEvent("4", 2, "payload4")
        val event5 = createEvent("5", 0, "payload5")
        val events = Seq(event1, event2, event3, event4, event5)

        worker.tell(ProcessEvents(events, mock[IRecordProcessorCheckpointer], "12345"),
                    managerProbe.ref)
        processorProbe.expectMsgAllOf(messageConfirmationTimeout,
                                      ProcessEvent(event1),
                                      ProcessEvent(event2),
                                      ProcessEvent(event3),
                                      ProcessEvent(event4),
                                      ProcessEvent(event5))

        managerProbe.expectMsg(batchConfirmationTimeout,
                               "Expecting successful ProcessingComplete",
                               ProcessingComplete())
      }

      "Should checkpoint the latest processed sequence number when the checkpointer is ready" - {
        "When the processed sequence numbers have no break in the sequence and are processed in order " in new Setup {

          val event1 = createEvent("1", 1, "payload1")
          val event2 = createEvent("1", 2, "payload2")
          val event3 = createEvent("3", 1, "payload3")
          val event4 = createEvent("4", 2, "payload4")
          val event5 = createEvent("5", 0, "payload5")
          val events = Seq(event1, event2, event3, event4, event5)

          val checkpointer = mock[IRecordProcessorCheckpointer]
          worker.tell(ProcessEvents(events, checkpointer, "12345"), managerProbe.ref)

          //We only check this to ensure we've processed all messages (we need the latest seq)
          processorProbe.expectMsgAllOf(messageConfirmationTimeout,
                                        ProcessEvent(event1),
                                        ProcessEvent(event2),
                                        ProcessEvent(event3),
                                        ProcessEvent(event4),
                                        ProcessEvent(event5))

          managerProbe.expectMsg(batchConfirmationTimeout,
                                 "Expecting successful ProcessingComplete",
                                 ProcessingComplete())

          checkpointerProbe.send(worker, ReadyToCheckpoint)

          checkpointerProbe.expectMsg(checkpointTimeout,
                                      "Expecting Checkpoint message",
                                      Checkpoint(checkpointer, event5.sequenceNumber))

          checkpointerProbe.reply(CheckpointResult(event5.sequenceNumber, success = true))

        }

        "When the messages are not processed in order, leading to a break in the confirmed sequence numbers" in new Setup {
          //Don't ack event 3
          override lazy val noAckSeqNos = Seq(CompoundSequenceNumber("3", 0))

          val event1 = createEvent("1", 0, "payload1")
          val event2 = createEvent("2", 0, "payload2")
          val event3 = createEvent("3", 0, "payload3")
          val event4 = createEvent("4", 0, "payload4")
          val event5 = createEvent("5", 0, "payload5")
          val events = Seq(event1, event3, event2, event5, event4)

          val checkpointer = mock[IRecordProcessorCheckpointer]
          worker.tell(ProcessEvents(events, checkpointer, "12345"), managerProbe.ref)

          //We only check this to ensure we've processed all messages
          processorProbe.expectMsgAllOf(messageConfirmationTimeout,
                                        ProcessEvent(event1),
                                        ProcessEvent(event2),
                                        ProcessEvent(event3),
                                        ProcessEvent(event4),
                                        ProcessEvent(event5))

          checkpointerProbe.send(worker, ReadyToCheckpoint)

          //The latest seq is 2 as we didn't ack 3.
          checkpointerProbe.expectMsg(checkpointTimeout,
                                      "Expecting Checkpoint message",
                                      Checkpoint(checkpointer, event2.sequenceNumber))

          checkpointerProbe.reply(CheckpointResult(event2.sequenceNumber, success = true))

          managerProbe.expectNoMessage(noMsgTimeout)
        }

        "When the messages are not processed in order for an aggregated batch" in new Setup {
          //Don't ack seq event3
          override lazy val noAckSeqNos = Seq(CompoundSequenceNumber("1", 3))

          //scalastyle:off magic.number
          val event1 = createEvent("1", 1, "payload1")
          val event2 = createEvent("1", 2, "payload2")
          val event3 = createEvent("1", 3, "payload3")
          val event4 = createEvent("1", 4, "payload4")
          val event5 = createEvent("1", 5, "payload5")

          val events = Seq(event1, event3, event2, event5, event4)

          val checkpointer = mock[IRecordProcessorCheckpointer]
          worker.tell(ProcessEvents(events, checkpointer, "12345"), managerProbe.ref)

          //We only check this to ensure we've processed all messages
          processorProbe.expectMsgAllOf(messageConfirmationTimeout,
                                        ProcessEvent(event1),
                                        ProcessEvent(event2),
                                        ProcessEvent(event3),
                                        ProcessEvent(event4),
                                        ProcessEvent(event5))

          checkpointerProbe.send(worker, ReadyToCheckpoint)

          //The latest seq is 2 as we didn't ack 3.
          checkpointerProbe.expectMsg(checkpointTimeout,
                                      "Expecting Checkpoint message",
                                      Checkpoint(checkpointer, event2.sequenceNumber))

          checkpointerProbe.reply(CheckpointResult(event2.sequenceNumber, success = true))

          managerProbe.expectNoMessage(noMsgTimeout)
        }

        "When a message is skipped by returning successful = false" in new Setup {
          //Don't ack seq 3
          override lazy val failedSeqNos = Seq(CompoundSequenceNumber("3", 1))

          val event1 = createEvent("1", 1, "payload1")
          val event2 = createEvent("1", 2, "payload2")
          val event3 = createEvent("3", 1, "payload3")
          val event4 = createEvent("4", 2, "payload4")
          val event5 = createEvent("5", 0, "payload5")
          val events = Seq(event1, event2, event3, event4, event5)

          val checkpointer = mock[IRecordProcessorCheckpointer]
          worker.tell(ProcessEvents(events, checkpointer, "12345"), managerProbe.ref)

          //We only check this to ensure we've processed all messages
          processorProbe.expectMsgAllOf(messageConfirmationTimeout,
                                        ProcessEvent(event1),
                                        ProcessEvent(event2),
                                        ProcessEvent(event3),
                                        ProcessEvent(event4),
                                        ProcessEvent(event5))

          checkpointerProbe.send(worker, ReadyToCheckpoint)

          //We decided to skip event 2, so 5 is the latest regardless.
          checkpointerProbe.expectMsg(checkpointTimeout,
                                      "Expecting Checkpoint message",
                                      Checkpoint(checkpointer, event5.sequenceNumber))

          checkpointerProbe.reply(CheckpointResult(event5.sequenceNumber, success = true))

          managerProbe.expectMsg(batchConfirmationTimeout,
                                 "Expecting successful ProcessingComplete",
                                 ProcessingComplete())
        }

        "When starting a new batch the old batch should be remembered" in new Setup {
          val checkpointer = mock[IRecordProcessorCheckpointer]

          //send the first batch
          val event1 = createEvent("1", 1, "payload1")
          worker.tell(ProcessEvents(Seq(event1), checkpointer, "12345"), managerProbe.ref)
          processorProbe.expectMsgAllOf(messageConfirmationTimeout, ProcessEvent(event1))
          managerProbe.expectMsg(batchConfirmationTimeout,
                                 "Expecting successful ProcessingComplete",
                                 ProcessingComplete())

          //we want to ensure the first batch will still be checkpointed if no more messages are processed
          //So process a second batch using a sequence number that won't be acked automatically
          //This way we'll start a new batch, but ensure the processed messages are carried over
          worker.tell(ProcessEvents(Seq(noAckRecord), checkpointer, "12345"), managerProbe.ref)
          processorProbe.expectMsgAllOf(messageConfirmationTimeout, ProcessEvent(noAckRecord))
          managerProbe.expectNoMessage(noMsgTimeout)

          //Fire checkpointer, we expect the latest from the first batch as the second batch hasn't yet been acked
          checkpointerProbe.send(worker, ReadyToCheckpoint)
          checkpointerProbe.expectMsg(checkpointTimeout,
                                      "Expecting Checkpoint message",
                                      Checkpoint(checkpointer, event1.sequenceNumber))
          checkpointerProbe.reply(CheckpointResult(event1.sequenceNumber, success = true))

          //Now ack the second batch
          processorProbe.send(worker, EventProcessed(noAckSeqNo))
          managerProbe.expectMsg(batchConfirmationTimeout,
                                 "Expecting successful ProcessingComplete",
                                 ProcessingComplete())

          //Validate that the second batch is now checkpointed
          checkpointerProbe.send(worker, ReadyToCheckpoint)
          checkpointerProbe.expectMsg(checkpointTimeout,
                                      "Expecting Checkpoint message",
                                      Checkpoint(checkpointer, noAckSeqNo))
          checkpointerProbe.reply(CheckpointResult(noAckSeqNo, success = true))
        }
      }

      "Should handle retries" - {
        "When one or more messages in the batch aren't confirmed in time, we should retry, confirming the messages" in new Setup {
          //Don't ack seq 3
          override lazy val noAckSeqNos       = Seq(CompoundSequenceNumber("3", 1))
          override lazy val batchRetries: Int = 1

          val event1 = createEvent("1", 1, "payload1")
          val event2 = createEvent("1", 2, "payload2")
          val event3 = createEvent("3", 1, "payload3")
          val event4 = createEvent("4", 2, "payload4")
          val event5 = createEvent("5", 0, "payload5")
          val events = Seq(event1, event2, event3, event4, event5)

          val checkpointer = mock[IRecordProcessorCheckpointer]
          worker.tell(ProcessEvents(events, checkpointer, "12345"), managerProbe.ref)

          //We only check this to ensure we've processed all messages
          processorProbe.expectMsgAllOf(messageConfirmationTimeout,
                                        ProcessEvent(event1),
                                        ProcessEvent(event2),
                                        ProcessEvent(event3),
                                        ProcessEvent(event4),
                                        ProcessEvent(event5))

          //the batch shouldn't be confirmed within the timeout
          managerProbe.expectNoMessage(batchTimeout)

          //we should then retry the unacked msg
          processorProbe.expectMsgAllOf(messageConfirmationTimeout, ProcessEvent(event3))

          //And we shouldn't checkpoint the unacked msg
          checkpointerProbe.send(worker, ReadyToCheckpoint)
          checkpointerProbe.expectMsg(checkpointTimeout,
                                      "Expecting Checkpoint message",
                                      Checkpoint(checkpointer, event2.sequenceNumber))
          checkpointerProbe.reply(CheckpointResult(event2.sequenceNumber, success = true))

          //Now ack the message
          processorProbe.send(worker, EventProcessed(event3.sequenceNumber))

          //Success!?!
          managerProbe.expectMsg(batchConfirmationTimeout,
                                 "Expecting successful ProcessingComplete",
                                 ProcessingComplete())

          //confirm the message now gets checkpointed!
          checkpointerProbe.send(worker, ReadyToCheckpoint)
          checkpointerProbe.expectMsg(checkpointTimeout,
                                      "Expecting Checkpoint message",
                                      Checkpoint(checkpointer, event5.sequenceNumber))
          checkpointerProbe.reply(CheckpointResult(event5.sequenceNumber, success = true))
        }

        "When one or more messages in the batch aren't confirmed, we should retry, eventually skipping if within tolerance" in new Setup {
          //Don't ack seq 3
          override lazy val noAckSeqNos              = Seq(CompoundSequenceNumber("3", 1))
          override lazy val batchRetries: Int        = 1
          override lazy val failureTolerance: Double = 20 //20 percent tolerance!! (1 in 5)

          val event1 = createEvent("1", 1, "payload1")
          val event2 = createEvent("1", 2, "payload2")
          val event3 = createEvent("3", 1, "payload3")
          val event4 = createEvent("4", 2, "payload4")
          val event5 = createEvent("5", 0, "payload5")
          val events = Seq(event1, event2, event3, event4, event5)

          val checkpointer = mock[IRecordProcessorCheckpointer]
          worker.tell(ProcessEvents(events, checkpointer, "12345"), managerProbe.ref)

          //We only check this to ensure we've processed all messages
          processorProbe.expectMsgAllOf(messageConfirmationTimeout,
                                        ProcessEvent(event1),
                                        ProcessEvent(event2),
                                        ProcessEvent(event3),
                                        ProcessEvent(event4),
                                        ProcessEvent(event5))

          //the batch shouldn't be confirmed within the timeout
          managerProbe.expectNoMessage(messageConfirmationTimeout)

          //we should then retry the unacked msg
          processorProbe.expectMsgAllOf(messageConfirmationTimeout, ProcessEvent(event3))

          //And we shouldn't checkpoint the unacked msg
          checkpointerProbe.send(worker, ReadyToCheckpoint)
          checkpointerProbe.expectMsg(checkpointTimeout,
                                      "Expecting Checkpoint message",
                                      Checkpoint(checkpointer, event2.sequenceNumber))
          checkpointerProbe.reply(CheckpointResult(event2.sequenceNumber, success = true))

          //still no confirmation? We're using a lower timeout to ensure we haven't timed out yet
          managerProbe.expectNoMessage(messageConfirmationTimeout)

          //Success (we should skip the message after the retry)!?!
          managerProbe.expectMsg(batchConfirmationTimeout,
                                 "Expecting successful ProcessingComplete",
                                 ProcessingComplete())

          //confirm the message now gets checkpointed!
          checkpointerProbe.send(worker, ReadyToCheckpoint)
          checkpointerProbe.expectMsg(checkpointTimeout,
                                      "Expecting Checkpoint message",
                                      Checkpoint(checkpointer, event5.sequenceNumber))
          checkpointerProbe.reply(CheckpointResult(event5.sequenceNumber, success = true))
        }

        "When one or more messages in the batch aren't confirmed, we should retry then fail processing if outside tolerance" in new Setup {
          //Don't ack seq 3
          override lazy val noAckSeqNos       = Seq(CompoundSequenceNumber("3", 1))
          override lazy val batchRetries: Int = 2 // retry twice just because..

          val event1 = createEvent("1", 1, "payload1")
          val event2 = createEvent("1", 2, "payload2")
          val event3 = createEvent("3", 1, "payload3")
          val event4 = createEvent("4", 2, "payload4")
          val event5 = createEvent("5", 0, "payload5")
          val events = Seq(event1, event2, event3, event4, event5)

          val checkpointer = mock[IRecordProcessorCheckpointer]
          worker.tell(ProcessEvents(events, checkpointer, "12345"), managerProbe.ref)

          //We only check this to ensure we've processed all messages
          processorProbe.expectMsgAllOf(messageConfirmationTimeout,
                                        ProcessEvent(event1),
                                        ProcessEvent(event2),
                                        ProcessEvent(event3),
                                        ProcessEvent(event4),
                                        ProcessEvent(event5))

          //the batch shouldn't be confirmed within the timeout
          managerProbe.expectNoMessage(messageConfirmationTimeout)

          // * RETRY 1 *
          //we should then retry the unacked msg
          processorProbe.expectMsgAllOf(messageConfirmationTimeout, ProcessEvent(event3))

          //still no confirmation? We're using a lower timeout to ensure we haven't timed out yet
          managerProbe.expectNoMessage(messageConfirmationTimeout)

          // * RETRY 2 *
          processorProbe.expectMsgAllOf(messageConfirmationTimeout, ProcessEvent(event3))
          managerProbe.expectNoMessage(messageConfirmationTimeout)

          //Expected batch failure!!
          managerProbe.expectMsg(batchConfirmationTimeout,
                                 "Expecting failed ProcessingComplete",
                                 ProcessingComplete(false))
          processorProbe.expectMsg(messageConfirmationTimeout,
                                   "Processor expecting EventProcessorFailure",
                                   ConsumerWorkerFailure(Seq(event3), "12345"))

          //And we shouldn't checkpoint the unacked msg
          checkpointerProbe.send(worker, ReadyToCheckpoint)
          checkpointerProbe.expectMsg(checkpointTimeout,
                                      "Expecting Checkpoint message",
                                      Checkpoint(checkpointer, event2.sequenceNumber))
          checkpointerProbe.reply(CheckpointResult(event2.sequenceNumber, success = true))
        }

      }

      "Should NOT checkpoint the same sequence number twice" in new Setup {
        val checkpointer = mock[IRecordProcessorCheckpointer]

        //send the batch
        val event1 = createEvent("1", 3, "payload1")
        worker.tell(ProcessEvents(Seq(event1), checkpointer, "12345"), managerProbe.ref)
        processorProbe.expectMsgAllOf(messageConfirmationTimeout, ProcessEvent(event1))
        managerProbe.expectMsg(batchConfirmationTimeout,
                               "Expecting successful ProcessingComplete",
                               ProcessingComplete())

        //validate the batch gets checkpointed
        checkpointerProbe.send(worker, ReadyToCheckpoint)
        checkpointerProbe.expectMsg(checkpointTimeout,
                                    "Expecting Checkpoint message",
                                    Checkpoint(checkpointer, event1.sequenceNumber))
        checkpointerProbe.reply(CheckpointResult(event1.sequenceNumber, success = true))

        //Ensure we don't checkpoint the same seq again
        checkpointerProbe.send(worker, ReadyToCheckpoint)
        checkpointerProbe.expectNoMessage(noMsgTimeout)
      }

      "Should NOT checkpoint if no messages have been confirmed" in new Setup {
        val checkpointer = mock[IRecordProcessorCheckpointer]

        //send the batch
        val event1 =
          createEvent(noAckSeqNo.sequenceNumber, noAckSeqNo.subSequenceNumber, "payloadNoAck")
        worker.tell(ProcessEvents(Seq(event1), checkpointer, "12345"), managerProbe.ref)
        processorProbe.expectMsgAllOf(messageConfirmationTimeout, ProcessEvent(event1))
        managerProbe.expectNoMessage(noMsgTimeout)

        //validate the batch doesn't get checkpointed
        checkpointerProbe.send(worker, ReadyToCheckpoint)
        checkpointerProbe.expectNoMessage(noMsgTimeout)
      }

      "Should NOT confirm the batch until all messages have been confirmed" in new Setup {
        val checkpointer = mock[IRecordProcessorCheckpointer]

        //By using the noAck SeqNo the autopilot won't respond to the worker
        worker.tell(ProcessEvents(Seq(noAckRecord), checkpointer, "12345"), managerProbe.ref)
        processorProbe.expectMsgAllOf(messageConfirmationTimeout, ProcessEvent(noAckRecord))
        //the batch shouldn't be acked with the manager yet
        managerProbe.expectNoMessage(noMsgTimeout)

        //Fire checkpointer, nothing should be checkpointed
        checkpointerProbe.send(worker, ReadyToCheckpoint)
        checkpointerProbe.expectNoMessage(noMsgTimeout)

        //Now ack the batch
        processorProbe.send(worker, EventProcessed(noAckSeqNo))
        //boom!
        managerProbe.expectMsg(batchConfirmationTimeout,
                               "Expecting successful ProcessingComplete",
                               ProcessingComplete())

        //Validate that the second batch is now checkpointed
        checkpointerProbe.send(worker, ReadyToCheckpoint)
        checkpointerProbe.expectMsg(checkpointTimeout,
                                    "Expecting Checkpoint message",
                                    Checkpoint(checkpointer, noAckSeqNo))
        checkpointerProbe.reply(CheckpointResult(noAckSeqNo, success = true))
      }

      "Should gracefully shutdown when no batch is processing, checkpointing and killing itself and the checkpointer" in new Setup {
        val checkpointer = mock[IRecordProcessorCheckpointer]

        managerProbe watch worker

        Given("A batch has been processed")
        //send the batch
        val event1 = createEvent("1", 2, "payload1")
        managerProbe.send(worker, ProcessEvents(Seq(event1), checkpointer, "12345"))
        processorProbe.expectMsgAllOf(messageConfirmationTimeout, ProcessEvent(event1))
        managerProbe.expectMsg(batchConfirmationTimeout,
                               "Expecting successful ProcessingComplete",
                               ProcessingComplete())

        When("We ask for a GracefulShutdown")
        managerProbe.send(worker, GracefulShutdown(checkpointer))

        Then("A Checkpoint is requested, forcing if necessary")
        //validate the batch gets checkpointed
        checkpointerProbe.expectMsg(checkpointTimeout,
                                    "Expecting Checkpoint message",
                                    Checkpoint(checkpointer, event1.sequenceNumber, force = true))

        When("A successful CheckpointResult is received")
        checkpointerProbe.reply(CheckpointResult(event1.sequenceNumber, success = true))

        Then("The Processor and Manager are notified")
        processorProbe.expectMsg(checkpointTimeout, ConsumerShutdown("12345"))
        managerProbe.expectMsg(checkpointTimeout, ShutdownComplete(true))

        And("The Consumer and Checkpoint workers are killed")
        checkpointerProbe.expectMsg(checkpointTimeout,
                                    "Expecting forced checkpoint message",
                                    "Actor Killed!")
        managerProbe.expectTerminated(worker)
      }

      "Should gracefully shutdown whilst processing a batch, checkpointing and killing itself and the checkpointer" in new Setup {
        val checkpointer = mock[IRecordProcessorCheckpointer]

        managerProbe watch worker

        Given("A 2 record batch is processing, but only one message has been acked")
        //send the batch
        val event1 = createEvent("1", 2, "payload1")
        managerProbe.send(worker, ProcessEvents(Seq(event1, noAckRecord), checkpointer, "12345"))
        processorProbe.expectMsgAllOf(messageConfirmationTimeout,
                                      ProcessEvent(event1),
                                      ProcessEvent(noAckRecord))

        When("We ask for a GracefulShutdown")
        managerProbe.send(worker, GracefulShutdown(checkpointer))

        Then("The manager is notified of batch completion (abort awaiting for batch response)")
        managerProbe.expectMsg(checkpointTimeout, ProcessingComplete(false))

        And("A Checkpoint is requested, forcing if necessary")
        //validate the batch gets checkpointed
        checkpointerProbe.expectMsg(checkpointTimeout,
                                    "Expecting Checkpoint message",
                                    Checkpoint(checkpointer, event1.sequenceNumber, force = true))

        When("A successful CheckpointResult is received")
        checkpointerProbe.reply(CheckpointResult(event1.sequenceNumber, success = true))

        Then("The Processor and Manager are notified")
        processorProbe.expectMsg(checkpointTimeout, ConsumerShutdown("12345"))
        managerProbe.expectMsg(checkpointTimeout, ShutdownComplete(true))

        And("The Consumer and Checkpoint workers are killed")
        checkpointerProbe.expectMsg(checkpointTimeout,
                                    "Expecting forced checkpoint message",
                                    "Actor Killed!")
        managerProbe.expectTerminated(worker)
      }

      "Should gracefully shutdown, handling checkpoint response timeout" in new Setup {
        val checkpointer = mock[IRecordProcessorCheckpointer]

        managerProbe watch worker

        Given("A batch has been processed")
        //send the batch
        val event1 = createEvent("1", 2, "payload1")
        managerProbe.send(worker, ProcessEvents(Seq(event1), checkpointer, "12345"))
        processorProbe.expectMsgAllOf(messageConfirmationTimeout, ProcessEvent(event1))
        managerProbe.expectMsg(batchConfirmationTimeout,
                               "Expecting successful ProcessingComplete",
                               ProcessingComplete())

        When("We ask for a GracefulShutdown")
        managerProbe.send(worker, GracefulShutdown(checkpointer))

        Then("A Checkpoint is requested, forcing if necessary")
        //validate the batch gets checkpointed
        checkpointerProbe.expectMsg(checkpointTimeout,
                                    "Expecting Checkpoint message",
                                    Checkpoint(checkpointer, event1.sequenceNumber, force = true))

        When("No CheckpointResult is received")

        Then("The Processor and Manager are notified")
        processorProbe.expectMsg(ConsumerShutdown("12345"))
        managerProbe.expectMsg(ShutdownComplete(false))

        And("The Consumer and Checkpoint workers are killed")
        checkpointerProbe.expectMsg(checkpointTimeout,
                                    "Expecting forced checkpoint message",
                                    "Actor Killed!")
        managerProbe.expectTerminated(worker)
      }

      "Should gracefully shutdown without checkpointing when no need to checkpoint" in new Setup {
        val checkpointer = mock[IRecordProcessorCheckpointer]

        managerProbe watch worker

        Given("A batch has been processed")
        //send the batch
        val event1 = createEvent("1", 2, "payload1")
        managerProbe.send(worker, ProcessEvents(Seq(event1), checkpointer, "12345"))
        processorProbe.expectMsgAllOf(messageConfirmationTimeout, ProcessEvent(event1))
        managerProbe.expectMsg(batchConfirmationTimeout,
                               "Expecting successful ProcessingComplete",
                               ProcessingComplete())

        And("The batch is checkpointed")
        //validate the batch gets checkpointed
        checkpointerProbe.send(worker, ReadyToCheckpoint)
        checkpointerProbe.expectMsg(checkpointTimeout,
                                    "Expecting Checkpoint message",
                                    Checkpoint(checkpointer, event1.sequenceNumber))
        checkpointerProbe.reply(CheckpointResult(event1.sequenceNumber, success = true))

        When("We ask for a GracefulShutdown")
        managerProbe.send(worker, GracefulShutdown(checkpointer))

        Then("A Checkpoint is not requested")

        And("The Processor and Manager are notified")
        processorProbe.expectMsg(ConsumerShutdown("12345"))
        managerProbe.expectMsg(ShutdownComplete(false))

        And("The Consumer and Checkpoint workers are killed")
        checkpointerProbe.expectMsg(checkpointTimeout,
                                    "Expecting forced checkpoint message",
                                    "Actor Killed!")
        checkpointerProbe.expectNoMessage(noMsgTimeout) // ensure ONLY the Actor Killed message was received
        managerProbe.expectTerminated(worker)
      }
    }

  }

  /**
    * Forwards any messages received by this actor to the passed probe.
    * This is used to allow us to pass a props to an actor whilst still monitoring the messages.
    */
  private class ChildForwarder(probe: ActorRef) extends Actor {

    def receive: PartialFunction[Any, Unit] = {
      case x => probe forward x
    }

    override def postStop(): Unit = {
      //We want to forward death onto the probe to track lifecycle
      probe ! "Actor Killed!"
    }
  }

  /**
    * Acks all [[ProcessEvent]] messages with a corresponding [[EventProcessed]]
    *
    * @param noAckSeqNos If the message contains this sequence number, ignore it.
    */
  private def processorSuccessfulAutoPilot(
      noAckSeqNos: Seq[CompoundSequenceNumber],
      failedSeqNos: Seq[CompoundSequenceNumber]
  ): AutoPilot = {
    new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): AutoPilot =
        msg match {
          case ProcessEvent(consumerEvent) if noAckSeqNos.contains(consumerEvent.sequenceNumber) =>
            TestActor.KeepRunning
          case ProcessEvent(consumerEvent) =>
            sender ! EventProcessed(
              consumerEvent.sequenceNumber,
              successful = if (failedSeqNos.contains(consumerEvent.sequenceNumber)) false else true
            )
            TestActor.KeepRunning
          case ConsumerWorkerFailure(_, _) =>
            TestActor.KeepRunning
          case ConsumerShutdown(_) =>
            TestActor.KeepRunning
          case m => fail(s"Processor received unexpected message from worker $m")
        }
    }
  }

  private def createEvent(seqNo: String, subNo: Long, payload: String): ConsumerEvent = {
    ConsumerEvent(CompoundSequenceNumber(seqNo, subNo),
                  ByteBuffer.wrap(payload.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                  new DateTime())
  }
}
