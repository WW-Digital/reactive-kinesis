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

import akka.Done
import akka.actor.Actor.Receive
import akka.actor.Status.Failure
import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.pipe
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, BufferOverflowException, Outlet, SourceShape}
import com.typesafe.scalalogging.LazyLogging
import com.weightwatchers.reactive.kinesis.consumer.ConsumerWorker.{
  ConsumerShutdown,
  ConsumerWorkerFailure,
  EventProcessed,
  ProcessEvent
}
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer.ConsumerConf
import com.weightwatchers.reactive.kinesis.models.ConsumerEvent

/**
  * The KinesisEvent is passed through the stream.
  * Every event has to be committed explicitly.
  */
sealed trait KinesisEvent {
  def event: ConsumerEvent
  def commit(successful: Boolean = true): KinesisEvent
}

/**
  * Actor based implementation of KinesisEvent.
  */
private[kinesis] case class KinesisActorEvent(event: ConsumerEvent)(implicit sender: ActorRef)
    extends KinesisEvent {
  def commit(successful: Boolean = true): KinesisEvent = {
    sender ! EventProcessed(event.sequenceNumber, successful)
    this
  }
}

/**
  * A KinesisSourceGraph will attach to a kinesis stream with the provided configuration and constitute a Source[KinesisEvent, NotUsed].
  * Usage:
  * {{{
  *   val config = ConfigFactory.load()
  *   val consumerConfig = ConsumerConf(config.getConfig("kinesis"), "some-consumer")
  *   val source = Source.fromGraph(new KinesisSourceGraph(consumerConf, system))
  * }}}
  * Assuming a configuration file like this:
  * {{{
  * kinesis {
  *    application-name = "SampleService"
  *    some-consumer {
  *       stream-name = "sample-consumer"
  *    }
  * }
  * }}}
  * See reference.conf for a list of all available config options.
  *
  * @param config the kinesis stream configuration.
  * @param actorSystem the actor system.
  */
class KinesisSourceGraph(config: ConsumerConf, actorSystem: ActorSystem)
    extends GraphStage[SourceShape[KinesisEvent]]
    with LazyLogging {

  private[this] val out: Outlet[KinesisEvent]   = Outlet("KinesisSource.out")
  override val shape: SourceShape[KinesisEvent] = SourceShape.of(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      // KCL will read events in batches. The stream should be able to buffer the whole batch.
      private[this] val bufferSize: Int = config.kclConfiguration.getMaxRecords
      // The queue to buffer events that can not be pushed downstream.
      private[this] val messages = new java.util.ArrayDeque[KinesisEvent]()
      // The kinesis consumer to read from.
      private[this] var kinesisConsumer: Option[KinesisConsumer] = None

      setHandler(out, new OutHandler {
        override def onPull(): Unit = if (!messages.isEmpty) push(out, messages.poll())
      })

      override def preStart(): Unit = {
        super.preStart()
        // the underlying stage actor reference of this graph stage.
        val actor    = getStageActor(receive).ref
        val consumer = KinesisConsumer(config, actor, actorSystem)
        // start() creates a long running future that returns, if the consumer worker is done or failed.
        import actorSystem.dispatcher
        consumer.start().map(_ => Done).pipeTo(actor)
        kinesisConsumer = Some(consumer)
      }

      override def postStop(): Unit = {
        logger.info(s"Stopping Source $out. ${messages.size()} messages are buffered unprocessed.")
        kinesisConsumer.foreach(_.stop())
        super.postStop()
      }

      def receive: Receive = {
        case (_, _: ProcessEvent) if messages.size > bufferSize =>
          // The messages are processed not fast enough, so the messages in the buffer exceeds the maximum buffersize
          // Fail the stage to prevent message overflow.
          // Ideally we could control when the next batch is fetched.
          logger.warn(s"Buffer of size $bufferSize is full. Fail the stream.")
          failStage(BufferOverflowException(s"Buffer overflow (max capacity was: $bufferSize)!"))

        case (actorRef: ActorRef, ProcessEvent(event)) =>
          // A new event that needs to be processed.
          // Always use the queue to guarantee the correct order of messages.
          logger.info(s"Process event: $event")
          messages.offer(KinesisActorEvent(event)(actorRef))
          while (isAvailable(out) && !messages.isEmpty) push(out, messages.poll())

        case (_, ConsumerShutdown(shardId)) =>
          // A consumer shutdown occurs when another source is created and hence the Kinesis shards are rebalanced.
          // This message is received, if the graceful shutdown is finalized.
          // Once https://github.com/WW-Digital/reactive-kinesis/issues/32 is fixed, we should drop all buffered
          // messages of this shard.
          logger.info(s"Consumer shutdown for shard $shardId")

        case (_, ConsumerWorkerFailure(failedEvents, shardId)) =>
          // Send for all events of a batch, where the processing has failed (after configured retries)
          // Since proceeding is not possible, the stream is failed.
          logger.error(s"Consumer worker failure for shard $shardId")
          failStage(
            new IllegalStateException(s"Failed Events: $failedEvents for shardId: $shardId")
          )

        case (_, Done) =>
          // The KinesisConsumer has been finished, so the stage is completed.
          logger.info("Kinesis Consumer finished.")
          completeStage()

        case (_, Failure(ex)) =>
          // The KinesisConsumer failed. This should also fail the stage.
          logger.error("Kinesis Consumer failed", ex)
          failStage(ex)
      }
    }
}
