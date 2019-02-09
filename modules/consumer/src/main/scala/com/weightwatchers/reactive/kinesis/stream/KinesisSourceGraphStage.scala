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
import com.weightwatchers.reactive.kinesis.consumer.{KinesisConsumer, ConsumerService}
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer.ConsumerConf
import com.weightwatchers.reactive.kinesis.models.{CompoundSequenceNumber, ConsumerEvent}
import org.joda.time.DateTime

import scala.concurrent.{ExecutionContext, Future}

/**
  * The CommittableEvent is passed through the stream.
  * Every event has to be committed explicitly.
  */
trait CommittableEvent[+A] {

  /**
    * The payload of this committable event.
    * @return the payload of this event.
    */
  def payload: A

  /**
    * The sequence number of this event.
    * @return the sequence number of this event.
    */
  def sequenceNumber: CompoundSequenceNumber

  /**
    * Timestamp when the event has been created.
    * @return timestamp of creation.
    */
  def timestamp: DateTime

  /**
    * Mark this event as handled. If all events of a batch are handled the read position can be advanced.
    * @param successful indicates if the event was handled successfully.
    */
  def commit(successful: Boolean = true): CommittableEvent[A]

  /**
    * Change the type of the payload by applying the given function to the payload.
    * This is useful if the payload is changed in different stages and the commit should be applied in a later stage.
    * Note: Committing a mapped event has the same effect as committing the original event - the sequence number is not changed.
    *
    * @param f the function to apply to the payload.
    * @tparam B the type of the returned CommittableEvent
    * @return A `CommittableEvent` with payload of type B
    */
  def map[B](f: A => B): CommittableEvent[B]

  /**
    * Change the type of the payload by applying the async function to the payload.
    * This is useful if the payload is changed in an async stage and the commit should be applied in a later stage.
    * Note: Committing a mapped event has the same effect as committing the original event - the sequence number is not changed.
    *
    * @param f the function to apply to the payload.
    * @param ec the execution context to use
    * @tparam B the type of the returned `CommittableEvent`
    * @return a `Future` which will be completed with a `CommittableEvent` with payload of type B
    */
  def mapAsync[B](f: A => Future[B])(implicit ec: ExecutionContext): Future[CommittableEvent[B]]
}

/**
  * Actor based implementation of CommittableEvent.
  */
private[kinesis] case class CommittableActorEvent[+A](event: ConsumerEvent, payload: A)(
    implicit sender: ActorRef
) extends CommittableEvent[A] {

  override def sequenceNumber: CompoundSequenceNumber = event.sequenceNumber

  override def timestamp: DateTime = event.timestamp

  def commit(successful: Boolean = true): CommittableEvent[A] = {
    sender ! EventProcessed(event.sequenceNumber, successful)
    this
  }
  override def map[B](f: A => B): CommittableEvent[B] = CommittableActorEvent(event, f(payload))

  override def mapAsync[B](
      f: A => Future[B]
  )(implicit ec: ExecutionContext): Future[CommittableEvent[B]] =
    f(payload).map(CommittableActorEvent(event, _))
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
  * @param createConsumer function that creates a consumer service from the graph stage actor reference.
  * @param actorSystem the actor system.
  */
class KinesisSourceGraphStage(config: ConsumerConf,
                              createConsumer: ActorRef => ConsumerService,
                              actorSystem: ActorSystem)
    extends GraphStage[SourceShape[CommittableEvent[ConsumerEvent]]]
    with LazyLogging {

  private[this] val out: Outlet[CommittableEvent[ConsumerEvent]]   = Outlet("KinesisSource.out")
  override val shape: SourceShape[CommittableEvent[ConsumerEvent]] = SourceShape.of(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      // KCL will read events in batches. The stream should be able to buffer the whole batch.
      private[this] val bufferSize: Int = config.kclConfiguration.getMaxRecords
      // The queue to buffer events that can not be pushed downstream.
      private[this] val messages = new java.util.ArrayDeque[CommittableEvent[ConsumerEvent]]()
      // The kinesis consumer to read from.
      private[this] var consumerService: Option[ConsumerService] = None

      setHandler(out, new OutHandler {
        override def onPull(): Unit = if (!messages.isEmpty) push(out, messages.poll())
      })

      override def preStart(): Unit = {
        super.preStart()
        // the underlying stage actor reference of this graph stage.
        val actor    = getStageActor(receive).ref
        val consumer = createConsumer(actor)
        // start() creates a long running future that returns, if the consumer worker is done or failed.
        import actorSystem.dispatcher
        consumer.start().map(_ => Done).pipeTo(actor)
        consumerService = Some(consumer)
      }

      override def postStop(): Unit = {
        logger.info(s"Stopping Source $out. ${messages.size()} messages are buffered unprocessed.")
        consumerService.foreach(_.stop())
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
          messages.offer(CommittableActorEvent[ConsumerEvent](event, event)(actorRef))
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
