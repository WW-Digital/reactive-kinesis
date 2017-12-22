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

import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.actor.{
  Actor,
  ActorInitializationException,
  ActorKilledException,
  ActorRef,
  Cancellable,
  OneForOneStrategy,
  PoisonPill,
  Props,
  UnboundedStash
}
import akka.event.LoggingReceive
import akka.pattern._
import akka.util.Timeout
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.weightwatchers.reactive.kinesis.consumer.CheckpointWorker.{
  Checkpoint,
  CheckpointResult,
  CheckpointerConf,
  ReadyToCheckpoint
}
import com.weightwatchers.reactive.kinesis.consumer.ConsumerWorker._
import com.weightwatchers.reactive.kinesis.models.{CompoundSequenceNumber, ConsumerEvent}
import scala.language.postfixOps

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Success

object ConsumerWorker {

  object ConsumerWorkerConf {

    /**
      * The config is expected to contain the following properties
      * {{{
      *      worker {
      *         batchTimeoutSeconds = 300
      *         failedMessageRetries = 1
      *         failureTolerancePercentage = 0.25
      *         shutdownTimeoutSeconds = 5
      *         gracefulShutdownHook = true
      *      }
      * }}}
      */
    def apply(consumerConfig: Config): ConsumerWorkerConf = {
      val shutdownHook         = consumerConfig.getBoolean("worker.gracefulShutdownHook")
      val shutdownTimeout      = consumerConfig.getInt("worker.shutdownTimeoutSeconds").seconds
      val batchTimeout         = consumerConfig.getInt("worker.batchTimeoutSeconds").seconds
      val failedMessageRetries = consumerConfig.getInt("worker.failedMessageRetries")
      val failureTolerancePercentage =
        consumerConfig.getDouble("worker.failureTolerancePercentage")

      new ConsumerWorkerConf(batchTimeout,
                             failedMessageRetries,
                             failureTolerancePercentage,
                             shutdownHook,
                             Timeout(shutdownTimeout))
    }
  }

  /**
    * Configuration for the Consumer Worker.
    *
    * @param batchTimeout               The total timeout for processing a single batch
    * @param failedMessageRetries       The number of times to retry failed batch messages
    * @param failureTolerancePercentage The delay between notification messages sent to the parent
    *                                   to indicate we're ready to checkpoint.
    * @param shutdownHook               Whether we automatically attempt a graceful shutdown on shutdown of the service
    * @param shutdownTimeout            When gracefully shutting down, this is the timeout allowed checkpointing etc
    */
  final case class ConsumerWorkerConf(batchTimeout: FiniteDuration,
                                      failedMessageRetries: Int,
                                      failureTolerancePercentage: Double,
                                      shutdownHook: Boolean,
                                      shutdownTimeout: Timeout)

  //TODO should these messages (sent to/from the processor) live in the models package?
  /**
    * Sent to the eventProcessor for each message in the batch.
    */
  case class ProcessEvent(consumerEvent: ConsumerEvent)

  /**
    * Expected in response to a [[ProcessEvent]] message after processing is complete
    *
    * @param compoundSeqNo This is a combination of the sequence and subsequence numbers
    * @param successful    Set this to false to skip this message.
    */
  case class EventProcessed(compoundSeqNo: CompoundSequenceNumber, successful: Boolean = true)

  /**
    * Sent to the eventProcessor if batch processing fails (above the tolerance after retrying)
    * before shutting down processing on this shard.
    *
    * @param failedEvents The events that failed processing within the time.
    * @param shardId      The shardId of the worker causing the failure.
    */
  case class ConsumerWorkerFailure(failedEvents: Seq[ConsumerEvent], shardId: String)

  /**
    * Sent to the eventProcessor upon shutdown of this worker.
    */
  case class ConsumerShutdown(shardId: String)

  /**
    * Sent from KinesisConsumer.
    *
    * @param records Batch of records to be processed.
    */
  private[consumer] case class ProcessEvents(records: Seq[ConsumerEvent],
                                             checkpointer: IRecordProcessorCheckpointer,
                                             shardId: String)

  private[consumer] case object ProcessingTimeout

  /**
    * We send this message to the manager upon completion of the batch
    */
  private[consumer] case class ProcessingComplete(successful: Boolean = true)

  /**
    * Handles any important shutdown requirements such as final checkpointing
    * @param checkpointer the checkpointer to use to checkpoint current position.
    */
  private[consumer] case class GracefulShutdown(checkpointer: IRecordProcessorCheckpointer)

  /**
    * Tells the manager we've completed shutdown
    */
  private[consumer] case class ShutdownComplete(checkpointSuccessful: Boolean)

  /**
    * Create an instance of the [[ConsumerWorker]] using the configuration values.
    *
    * @param eventProcessor   See [[ConsumerWorker]]
    * @param workerConf       the configuration for the worker
    * @param checkpointerConf A case class containing all checkpointer config. @see [[CheckpointWorker]]
    * @param dispatcher       an optional dispatcher to be used by this worker's checkpointer
    */
  private[consumer] def props(eventProcessor: ActorRef,
                              workerConf: ConsumerWorkerConf,
                              checkpointerConf: CheckpointerConf,
                              dispatcher: Option[String]): Props = {

    val checkpointerProps = CheckpointWorker.props(checkpointerConf)

    //Specify the dispatcher according to the config
    Props(classOf[ConsumerWorker],
          eventProcessor,
          workerConf,
          dispatcher.fold(checkpointerProps)(checkpointerProps.withDispatcher))
  }

}

/**
  * This actor statefully processes records emitted by KinesisConsumer.
  *
  * This actor is specific to one shard, it is 1:1 with the [[ConsumerProcessingManager]] instance.
  *
  * Points of note:
  * - The eventProcessor MUST handle [[ProcessEvent]] messages (for each message)
  * - The eventProcesser MUST respond with [[EventProcessed]] after processing of the [[ProcessEvent]]
  * - The eventProcessor may set `successful` to false to indicate the message can be skipped
  * - The eventProcesser SHOULD handle [[ConsumerWorkerFailure]] messages which signal a critical failure in the Consumer.
  * - The eventProcessor SHOULD handle [[ConsumerShutdown]] messages which siganl a graceful shutdown of the Consumer.
  * - Unconfirmed messages will be retried according to the configuration
  * - After all retries, if processing is still unsuccessful we move to the next batch if we're under the `failureTolerancePercentage`
  * - If we're above the `failureTolerancePercentage` then we stop processing message on this shard and send a [[ConsumerWorkerFailure]] to the eventProcesser
  * - Checkpointing takes place asynchronously and is not related to batch processing (it can span batches)
  *
  * @param eventProcessor This actor will handle processing of individual events. The user is responsible for providing this.
  *                       The [[ProcessEvent]] message MUST be handled by the actor and will be sent per event.
  *
  *                       Once processing of the event is complete, the actor MUST response with an [[EventProcessed]].
  *                       Upon receipt of this message the sequence number will be used for the next checkpoint.
  *
  *                       If `sucessful = false` is set in the [[EventProcessed]] message,
  *                       then we will treat this as processed in the same way.
  */
private[consumer] class ConsumerWorker(eventProcessor: ActorRef,
                                       workerConf: ConsumerWorkerConf,
                                       checkpointerProps: Props)
    extends Actor
    with UnboundedStash
    with LazyLogging {

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 1 minute) {
      //TODO let's be more specific here...
      case _: ActorInitializationException => Stop
      case _: ActorKilledException         => Stop
      case _: Exception                    => Restart
    }

  implicit val ec = context.dispatcher

  /**
    * Keep track of processed records that are ready to checkpoint (sequence numbers are per shard / checkpointer).
    * This generally lives for the live of a batch.
    */
  private class ResponseCollector(expectedResponses: Seq[ConsumerEvent],
                                  previouslyReceivedHighestSeq: Option[CompoundSequenceNumber] =
                                    None) {

    private val batchSequenceNumbers: collection.mutable.SortedSet[CompoundSequenceNumber] =
      collection.mutable.SortedSet(expectedResponses.map(_.sequenceNumber): _*)(
        CompoundSequenceNumber.orderingBySeqAndSubSeq.reverse
      )

    private val receivedResponseSeqNos: collection.mutable.SortedSet[CompoundSequenceNumber] =
      collection.mutable.SortedSet
        .empty[CompoundSequenceNumber](CompoundSequenceNumber.orderingBySeqAndSubSeq.reverse)

    /**
      * Called when a response is received, recording the sequenceNumber.
      */
    def receivedResponse(sequenceNumber: CompoundSequenceNumber): Unit = {
      receivedResponseSeqNos += sequenceNumber
    }

    /**
      * True if we've had confirmation of all messages
      */
    def isDone: Boolean = batchSequenceNumbers.size <= receivedResponseSeqNos.size

    /**
      * Returns the latest processed response based on a continuous sequence of processed messages.
      *
      * For example:
      * {{{
      * 1,2,3,4,5,6 = Some(6)
      * 1,2,4,5,6 = Some(2) (3 isn't yet confirmed)
      * 1 = Some(1)
      * [] = None
      * }}}
      */
    def latestConfirmedEventSeq: Option[CompoundSequenceNumber] = {
      if (isDone) batchSequenceNumbers.headOption
      else {
        //unprocessed messages, still in reverse order
        val unprocessed: collection.mutable.SortedSet[CompoundSequenceNumber] =
          internalUnconfirmedEvents

        //From left to right (in the reversed Set, drop all elements up to and including the last unprocessed)
        //This gives us the latest successfully processed Seq
        val lastUnprocessed = unprocessed.last

        val latestContinousProcessedSeq =
          batchSequenceNumbers.dropWhile(_ >= lastUnprocessed).headOption

        //If we have nothing then revert to the previous batch
        latestContinousProcessedSeq.orElse(previouslyReceivedHighestSeq)
      }
    }

    /**
      * This ignores whether the events have been processed and simply returns the latest
      */
    def latestEventSeq: Option[CompoundSequenceNumber] = batchSequenceNumbers.headOption

    def totalEventsCount: Int = batchSequenceNumbers.size

    /**
      * Exposes an immutable [[Set]] of the unconfirmed [[ConsumerEvent]].
      */
    def unconfirmedEvents: Seq[ConsumerEvent] = {
      val ucEvents = internalUnconfirmedEvents
      expectedResponses.filter(e => ucEvents.contains(e.sequenceNumber))
    }

    private def internalUnconfirmedEvents: collection.mutable.SortedSet[CompoundSequenceNumber] = {
      batchSequenceNumbers diff receivedResponseSeqNos
    }
  }

  private val checkpointWorker =
    context.actorOf(checkpointerProps, s"checkpoint-worker-${UUID_GENERATOR.generate()}")

  /**
    * We're making the assumption that the latest checkpointer given to us can be used to checkpoint for the whole shard.
    * The alternative is to track all checkpointers -> seqNo's in a Map and checkpoint against each one, however
    * as this worker is shard specific it can be assumed that any new checkpointer would replace the old ones.
    */
  private var latestCheckpointer: Option[IRecordProcessorCheckpointer] = None

  private var lastCheckpointedSeq: Option[CompoundSequenceNumber] = None
  private var latestShardId: Option[String]                       = None
  private var batchProcessingTimeoutTimer: Option[Cancellable]    = None

  override def receive: Receive = readyToProcess(None)

  /**
    * The waiting state, we're ready for the next batch (or to checkpoint).
    *
    * @param previouslyReceivedHighestSeq Carries over the latest Sequence number from the previous batch.
    */
  private def readyToProcess(
      previouslyReceivedHighestSeq: => Option[CompoundSequenceNumber]
  ): Receive = LoggingReceive {

    logger.trace(
      s"Worker for shard $latestShardId: in ready state, at seqNo $previouslyReceivedHighestSeq"
    )

    receiveCheckpoint(previouslyReceivedHighestSeq, None).orElse {
      case ProcessEvents(events, checkpointer, shardId) => {
        latestCheckpointer = Some(checkpointer)
        latestShardId = Some(shardId)

        context.become(
          processing(new ResponseCollector(events, previouslyReceivedHighestSeq), sender())
        )

        //Notify the processor!
        events.foreach {
          eventProcessor ! ProcessEvent(_)
        }

        restartBatchProcessingTimeoutTimer()
      }

      case m =>
        logger.warn(
          s"Worker for shard $latestShardId: Unexpected message received by ConsumerWorker whilst in Ready state: $m"
        )
    }
  }

  /**
    * We are currently processing a Kinesis record.
    * While we're processing this record, any additional requests to process records need to be queued up in our mailbox.
    * Once done, we need to receive a message that tells us the processing is complete.
    * Once done, if enough time has elapsed, we should checkpoint.
    */
  //scalastyle:off method.length
  private def processing(responseCollector: ResponseCollector,
                         manager: ActorRef,
                         retryAttempt: Int = 0): Receive = LoggingReceive {

    def switchToReadyState(latestProcessedSeq: Option[CompoundSequenceNumber]) = {
      context.become(readyToProcess(latestProcessedSeq))
      unstashAll()
    }

    receiveCheckpoint(responseCollector.latestConfirmedEventSeq, Some(manager)).orElse {
      case EventProcessed(sequenceNo, successful) =>
        responseCollector.receivedResponse(sequenceNo)

        if (!successful)
          logger.warn(
            s"Worker for shard $latestShardId: Skipping message SequenceNumber '$sequenceNo' due to client failure"
          )

        if (responseCollector.isDone) {
          cancelBatchProcessingTimeoutTimer()
          logger.trace(s"Worker for shard $latestShardId: successfully completed batch")
          manager ! ProcessingComplete()
          switchToReadyState(responseCollector.latestConfirmedEventSeq)
        } else {
          context.become(processing(responseCollector, manager))
        }

      case ProcessingTimeout if retryAttempt < workerConf.failedMessageRetries =>
        val unconfirmedResponses = responseCollector.unconfirmedEvents
        logger.debug(
          s"Worker for shard $latestShardId: Timed out processing batch, retrying ${unconfirmedResponses.size} messages"
        )
        restartBatchProcessingTimeoutTimer()
        context.become(processing(responseCollector, manager, retryAttempt + 1))
        unconfirmedResponses.foreach {
          eventProcessor ! ProcessEvent(_) //try again!!
        }

      case ProcessingTimeout =>
        val unconfirmedResponses = responseCollector.unconfirmedEvents
        logger.warn(
          s"Worker for shard $latestShardId: Timed out processing batch, failed to confirm processing for: $unconfirmedResponses"
        )
        cancelBatchProcessingTimeoutTimer() //to be sure

        if (unconfirmedResponses.size <= allowedFailures(responseCollector.totalEventsCount)) {
          val latestSeq = responseCollector.latestEventSeq // Just pretend all events were confirmed
          logger.warn(s"Ignoring unconfirmed messages, setting next checkpoint at $latestSeq")
          manager ! ProcessingComplete()
          switchToReadyState(latestSeq)
        } else {
          logger.warn(s"Worker for shard $latestShardId: Shutting down shard worker on this node")
          eventProcessor ! ConsumerWorkerFailure(unconfirmedResponses,
                                                 latestShardId.getOrElse("-"))
          manager ! ProcessingComplete(successful = false)
          //Move back to ready, although we don't expect to process another batch - that's up to the manager
          switchToReadyState(responseCollector.latestConfirmedEventSeq)
        }

      case m =>
        logger.info(
          s"Worker for shard $latestShardId: Unexpected message received by ConsumerWorker whilst in processing state, stashing: $m"
        )
        stash()
    }
  }

  /**
    * Handles all our checkpointing and shutdown logic.
    */
  private def receiveCheckpoint(latestProcessedSeq: => Option[CompoundSequenceNumber],
                                manager: Option[ActorRef]): Receive = {

    case ReadyToCheckpoint =>
      checkpoint(latestProcessedSeq) {
        (checkpointer: IRecordProcessorCheckpointer, latestSeq: CompoundSequenceNumber) =>
          checkpointWorker ! Checkpoint(checkpointer, latestSeq)
      }

    case CheckpointResult(seqNo, success) =>
      logger.trace(s"Worker for shard $latestShardId: Received Checkpointresult for : $seqNo")

      //TODO move this "should we checkpoint" logic into the checkpoint worker?
      if (success && isLaterThanLastCheckpoint(seqNo)) {
        logger.trace(
          s"Worker for shard $latestShardId: Updated latest checkpointed record after successfully processing : $seqNo"
        )
        lastCheckpointedSeq = Some(seqNo)
      } else if (!success) {
        //TODO We could potentially add more info to this message so we can decide whether the failure critical or not
        logger.warn(s"Worker for shard $latestShardId: Failed to checkpoint '$seqNo'")
      }

    case GracefulShutdown(shutdownCheckpointer) => {
      implicit val shutdownTimeout = workerConf.shutdownTimeout
      val outerSender              = sender()

      context.become(shuttingDown())

      (batchProcessingTimeoutTimer, manager) match {
        case (Some(_), Some(managerRef)) =>
          logger.info(
            s"Worker for shard $latestShardId: Cancelling processing of current batch, only previously acked messages will be checkpointed"
          )
          cancelBatchProcessingTimeoutTimer()
          managerRef ! ProcessingComplete(false)
        case _ =>
      }

      //Note that if this checkpoint fails (backoff, etc), we won't retry!!
      checkpoint(latestProcessedSeq) {
        (_: IRecordProcessorCheckpointer, latestSeq: CompoundSequenceNumber) =>
          (checkpointWorker ? Checkpoint(shutdownCheckpointer, latestSeq, force = true))
            .mapTo[CheckpointResult]
      } match {
        case Some(cpResponseFut) =>
          cpResponseFut.onComplete {
            case Success(cpResponse) =>
              //Once we're done checkpointing, continue shutdown and notify the manager (which is waiting)
              finaliseShutdown(outerSender, cpResponse.success)
            case _ =>
              logger.warn(s"Worker for shard $latestShardId: Failed to checkpoint on shutdown")
              finaliseShutdown(outerSender, false)
          }
        case None =>
          logger.warn(s"Worker for shard $latestShardId: Skipped checkpointing on shutdown")
          finaliseShutdown(outerSender, false)
      }

      /*
       * Complete graceful shutdown:
       * * Notify the manager of completion
       * * Notify the  eventProcessor in the consuming application of the imminent shutdown,
       * * Kill the checkpointer
       * * Kill self
       */
      def finaliseShutdown(manager: ActorRef, checkpointed: Boolean): Unit = {
        manager ! ShutdownComplete(checkpointed)
        eventProcessor ! ConsumerShutdown(latestShardId.getOrElse("-"))
        checkpointWorker ! PoisonPill
        self ! PoisonPill
      }
    }

  }

  private def shuttingDown(): Receive = {
    case m =>
      logger.debug(
        s"Worker for shard $latestShardId: message received by ConsumerWorker whilst shutting down: $m"
      )
  }

  /**
    * Use the provided function to checkpoint.
    *
    * @return Some if checkpointed, None otherwise (generally this means we don't need to checkpoint, it's not a failure).
    */
  private def checkpoint[T](
      latestProcessedSeq: Option[CompoundSequenceNumber]
  )(doCheckpoint: (IRecordProcessorCheckpointer, CompoundSequenceNumber) => T): Option[T] = {
    for {
      checkpointer <- latestCheckpointer
      latestSeq    <- latestProcessedSeq
      if isLaterThanLastCheckpoint(latestSeq)
    } yield doCheckpoint(checkpointer, latestSeq)
  }

  private def allowedFailures(allEventsCount: Int): Int = {
    ((workerConf.failureTolerancePercentage / 100) * allEventsCount).toInt
  }

  private def isLaterThanLastCheckpoint(sequenceNo: CompoundSequenceNumber) = {
    lastCheckpointedSeq.isEmpty || lastCheckpointedSeq.last < sequenceNo
  }

  private def cancelBatchProcessingTimeoutTimer(): Unit = {
    batchProcessingTimeoutTimer.map(_.cancel())
    batchProcessingTimeoutTimer = None
  }

  private def restartBatchProcessingTimeoutTimer(): Unit = {
    logger.trace(
      s"Worker for shard $latestShardId: Restarting Consumer Worker timer, set to ${workerConf.batchTimeout}"
    )
    cancelBatchProcessingTimeoutTimer()
    batchProcessingTimeoutTimer = Some(
      context.system.scheduler
        .scheduleOnce(workerConf.batchTimeout, self, ProcessingTimeout)(context.dispatcher)
    )
  }
}
