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

import akka.actor.{Actor, Cancellable, Props, UnboundedStash}
import akka.event.LoggingReceive
import com.amazonaws.services.kinesis.clientlibrary.exceptions.{
  InvalidStateException,
  ShutdownException,
  ThrottlingException
}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.weightwatchers.reactive.kinesis.consumer.CheckpointWorker._
import com.weightwatchers.reactive.kinesis.models.CompoundSequenceNumber

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object CheckpointWorker {

  object CheckpointerConf {

    /**
      * The config is expected to contain the following properties
      * {{{
      *      checkpointer {
      *         backoffMillis = 3000
      *         intervalMillis = 20000
      *         notificationDelayMillis = 2
      *      }
      * }}}
      */
    def apply(consumerConfig: Config): CheckpointerConf = {
      val backoffMillis  = consumerConfig.getInt("checkpointer.backoffMillis").millis
      val intervalMillis = consumerConfig.getInt("checkpointer.intervalMillis").millis
      val notificationDelayMillis =
        consumerConfig.getInt("checkpointer.notificationDelayMillis").millis

      new CheckpointerConf(backoffMillis, intervalMillis, notificationDelayMillis)
    }
  }

  /**
    * Configuration for the checkpointer.
    *
    * @param backoff           The amount of time to wait after failing to checkpoint
    * @param interval          The standard delay between (successful) checkpoints
    * @param notificationDelay The delay between notification messages sent to the parent to indicate we're ready to checkpoint
    *                          This is for subsequent messages after the initial notification,
    *                          for cases where the parent had no messages to checkpoint.
    */
  final case class CheckpointerConf(backoff: FiniteDuration,
                                    interval: FiniteDuration,
                                    notificationDelay: FiniteDuration)

  /**
    * This message is sent to the parent when the [[CheckpointWorker]] is ready to process the next [[Checkpoint]] message.
    */
  private[consumer] case object ReadyToCheckpoint

  /**
    * Accepts the sequence number of the latest record processed, along with the appropriate checkpointer.
    *
    * @param force can be used to force an immediate checkpoint regardless of the current state.
    */
  private[consumer] case class Checkpoint(checkpointer: IRecordProcessorCheckpointer,
                                          sequenceNo: CompoundSequenceNumber,
                                          force: Boolean = false)

  /**
    * One per entry in the [[Checkpoint]] message will be returned, indicating the success of the checkpoint.
    */
  private[consumer] case class CheckpointResult(sequenceNumber: CompoundSequenceNumber,
                                                success: Boolean)

  /**
    * Returned when checkponting is attempted without being initialted.
    */
  private[consumer] case class NotReady(checkpoint: Checkpoint)

  /** Internal Objects **/
  private case object AcceptCheckpointRequests

  /**
    * Creates a props for the CheckpointWorker.
    *
    * @param checkpointerConf A case class containing all checkpointer config.
    */
  private[consumer] def props(checkpointerConf: CheckpointerConf): Props = {
    Props(classOf[CheckpointWorker],
          checkpointerConf.backoff,
          checkpointerConf.interval,
          checkpointerConf.notificationDelay)
  }
}

/**
  * Handles checkpointing of records. Will request the next Checkpoint message by sending ReadyToCheckpoint.
  * Each [[ConsumerWorker]] has it's own [[CheckpointWorker]], they are NOT shared.
  */
private[consumer] class CheckpointWorker(backOffTime: FiniteDuration,
                                         checkpointInterval: FiniteDuration,
                                         notificationDelay: FiniteDuration)
    extends Actor
    with UnboundedStash
    with LazyLogging {

  // notifies the ConsumerWorker
  private var nextCheckpointTimer: Option[Cancellable] = None
  // internal timer for scheduling the next checkpoint state
  private var checkpointNotificationTimer: Option[Cancellable] = None

  override def receive: Receive = readyToCheckpoint()

  private def readyToCheckpoint(): Receive = LoggingReceive {
    logger.trace("Ready to Checkpoint...")

    cancelTimers()
    // continually remind the ConsumerWorker we're ready!
    checkpointNotificationTimer = Some(
      context.system.scheduler.scheduleWithFixedDelay(notificationDelay,
                                                      notificationDelay,
                                                      context.parent,
                                                      ReadyToCheckpoint)(context.dispatcher)
    )

    def readyState: Receive = LoggingReceive {
      case Checkpoint(checkpointer, sequenceNo, force) => {
        context.become(checkpointing())
        checkpointAndRespond(checkpointer, sequenceNo, force) //TODO we're ignoring failed checkpoints
      }
    }

    readyState
  }

  /**
    * When we're checkpointing we don't accept other messages.
    */
  private def checkpointing(): Receive = LoggingReceive {
    logger.trace(s"Checkpointing...")

    cancelTimers()
    handleUnexpectedMessages("checkpointing")
  }

  /**
    * We encountered a "throttling" exception from Dynamo, or we've just processed some messages.
    * So we need to wait a little while until we resume checkpointing.
    */
  private def waiting(timeToWait: FiniteDuration): Receive = LoggingReceive {
    logger.trace(s"Waiting ${timeToWait.toSeconds} seconds before next checkpoint")

    nextCheckpointTimer = Some(
      context.system.scheduler
        .scheduleOnce(timeToWait, self, AcceptCheckpointRequests)(context.dispatcher)
    )

    def waitingState: Receive = LoggingReceive {
      case AcceptCheckpointRequests =>
        context.become(readyToCheckpoint())
    }

    waitingState.orElse(handleUnexpectedMessages("waiting"))
  }

  private def cancelTimers(): Unit = {
    nextCheckpointTimer.map(_.cancel())
    nextCheckpointTimer = None
    checkpointNotificationTimer.map(_.cancel())
    checkpointNotificationTimer = None
  }

  /**
    * Initiates a checkpoint for every record sequentially. If any one record fails then no more will be processed.
    * A response is sent to the parent for every record, along with a success state.
    */
  private def checkpointAndRespond(checkpointer: IRecordProcessorCheckpointer,
                                   sequenceNo: CompoundSequenceNumber,
                                   shuttingDown: Boolean): Unit = {
    logger.trace(
      s">>> Checkpoint message for checkpointer $checkpointer, record seqNo $sequenceNo"
    )

    cancelTimers()

    //Once we've failed we don't want to keep processing until after the backoff
    val success = checkpointOrBackoff(checkpointer, sequenceNo)
    sender() ! CheckpointResult(sequenceNo, success)

    if (!shuttingDown) scheduleNextCheckpoint(success)
  }

  /**
    * Schedules the next checkpoint by switching to the wait context, using the appropriate timeout.
    */
  private def scheduleNextCheckpoint(success: Boolean): Unit = {
    //TODO we're assuming we always backoff when the checkpoint failed...
    if (success) {
      context.become(waiting(checkpointInterval))
    } else {
      context.become(waiting(backOffTime))
    }
  }

  /**
    * Checkpoint the given sequence number using the checkpointer.
    * Will return true if processed successfully, false if not - including when a backoff is required.
    */
  private def checkpointOrBackoff(checkpointer: IRecordProcessorCheckpointer,
                                  recordSequenceNumber: CompoundSequenceNumber): Boolean = {

    val checkpointResult = Try {
      checkpointer.checkpoint(recordSequenceNumber.sequenceNumber,
                              recordSequenceNumber.subSequenceNumber)
    }

    checkpointResult match {
      case Success(_) =>
        true

      case Failure(_: ThrottlingException) =>
        logger.info(s"Throttled by DynamoDB on checkpointing -- backing off...")
        false

      case Failure(e: InvalidStateException) =>
        // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
        logger.error(
          "Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.",
          e
        )
        // TODO propagate this to parent supervisor so that the app can decide whether to stop processing??
        false

      case Failure(se: ShutdownException) =>
        // Ignore checkpoint if the processor instance has been shutdown (fail over).
        logger.info("Caught shutdown exception, skipping checkpoint.", se)
        false

      case Failure(ex) =>
        logger.error("Caught unexpected exception, skipping checkpoint.", ex)
        // TODO propagate this to parent supervisor?
        false
    }
  }

  /**
    * When we're not expecting a checkpoint message we just log and respond with [[NotReady]].
    */
  private def handleUnexpectedMessages(state: String): Receive = LoggingReceive {
    case cp @ Checkpoint(checkpointer, sequenceNo, force) =>
      logger.warn(s"Received unexpected checkpoint message $cp in state $state")
      if (force) {
        logger.info(s"Forcing checkpoint: $cp")
        checkpointAndRespond(checkpointer, sequenceNo, force)
      } else {
        sender() ! NotReady(cp)
      }
    case anyMsg =>
      logger.warn(s"Received unexpected message in Kinesis CheckpointWorker ${anyMsg.getClass}")
  }
}
