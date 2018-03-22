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

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{
  IRecordProcessor,
  IShutdownNotificationAware
}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.amazonaws.services.kinesis.clientlibrary.types._
import com.amazonaws.services.kinesis.model.Record
import com.typesafe.scalalogging.LazyLogging
import com.weightwatchers.reactive.kinesis.consumer.ConsumerWorker.{
  GracefulShutdown,
  ProcessEvents,
  ProcessingComplete
}
import com.weightwatchers.reactive.kinesis.models.{CompoundSequenceNumber, ConsumerEvent}
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.JavaConverters._
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

/**
  * Manages the processing of messages for a SPECIFIC SHARD.
  * This means we have one instance of this class per shard that we're consuming from.
  *
  * @param consumerWorker  The [[ConsumerWorker]] which handles the processing completion logic and checkpointing (not shared between managers).
  * @param batchTimeout    The timeout for processing an individual batch.
  * @param shutdownTimeout The timeout for on shutdown.
  */
private[consumer] class ConsumerProcessingManager(
    consumerWorker: ActorRef,
    kclWorker: Worker,
    implicit val batchTimeout: Timeout,
    shutdownTimeout: FiniteDuration = 25.seconds
)(implicit ec: ExecutionContext)
    extends IRecordProcessor
    with IShutdownNotificationAware
    with LazyLogging {

  logger.info(
    s"Created shard specific ConsumerProcessingManager for worker: ${kclWorker.getApplicationName}:${kclWorker.toString}"
  )

  private[consumer] var kinesisShardId: String = ""
  private[consumer] val shuttingDown           = new AtomicBoolean(false)

  override def initialize(initializationInput: InitializationInput): Unit = {
    logger.info(
      "Initializing ConsumerProcessingManager for shard: " + initializationInput.getShardId
    )
    this.kinesisShardId = initializationInput.getShardId
  }

  /**
    * Each shard will be processed by its own AWS-level worker, and each will have it's own instance of this class.
    * Therefore we can assume this method is called by only one concurrent thread (we block to ensure this is the case).
    */
  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    def toConsumerEvent(record: Record): ConsumerEvent = {
      val userRecord = record.asInstanceOf[UserRecord]
      ConsumerEvent(
        CompoundSequenceNumber(userRecord.getSequenceNumber, userRecord.getSubSequenceNumber),
        userRecord.getData,
        new DateTime(userRecord.getApproximateArrivalTimestamp, DateTimeZone.UTC)
      )
    }

    if (shuttingDown.get) {
      logger.info(s"Skipping batch due to pending shutdown for shard: $kinesisShardId")
    } else {

      logger.info(
        s"Processing ${processRecordsInput.getRecords.size()} records from shard: $kinesisShardId"
      )

      // Process records and perform all exception handling.
      val allRecordsProcessedFut: Future[ProcessingComplete] =
        (consumerWorker ? ProcessEvents(
          processRecordsInput.getRecords.asScala.map(toConsumerEvent),
          processRecordsInput.getCheckpointer,
          kinesisShardId
        )).mapTo[ProcessingComplete]

      // Not great, but we don't have another option, we need to block!
      // The AWS library has already wrapped this in a task and if we fire-and-forget the processing of records,
      // we'll eventually run out of resources as this will be called continually according to the `idleTimeBetweenReadsInMillis` setting.
      Try {
        logger.debug(s"Consumer Awaiting batch result from worker, timeout is $batchTimeout")
        val allRecordsProcessed: ProcessingComplete =
          Await.result(allRecordsProcessedFut, batchTimeout.duration)
        if (!allRecordsProcessed.successful) {
          closeManager()
        }
      } recover {
        case exception =>
          logger.error(s"Unexpected Error processing records, shutting down worker", exception)
          closeManager()
      }
    }
  }

  override def shutdownRequested(checkpointer: IRecordProcessorCheckpointer): Unit = {
    logger.info(s"Graceful shutdown requested for record processor of shard: $kinesisShardId.")
    shutdown(checkpointer)
  }

  override def shutdown(shutdownInput: ShutdownInput): Unit = {
    logger.info(
      s"Shutdown record processor for shard: $kinesisShardId. Reason: ${shutdownInput.getShutdownReason}"
    )
    shutdown(shutdownInput.getCheckpointer)
  }

  private[consumer] def shutdown(checkpointer: IRecordProcessorCheckpointer): Unit = {
    if (shuttingDown.compareAndSet(false, true)) {
      logger.info(s"*** Shutting down record processor for shard: $kinesisShardId ***")

      Try {
        Await.result(consumerWorker ? GracefulShutdown(checkpointer), shutdownTimeout)
      } recover {
        case exception =>
          logger.error(
            s"Unexpected exception on shutdown, final checkpoint attempt may have failed",
            exception
          )
      }
    } else {
      logger.warn(s"Shutdown already initiated for record processor of shard: $kinesisShardId.")
    }
  }

  private[consumer] def closeManager(): Unit = {
    val canCloseManager = shuttingDown.compareAndSet(false, true)
    if (canCloseManager) {
      Future(kclWorker.startGracefulShutdown()) //Needs to be async otherwise we hog the processRecords thread
    }
  }
}
