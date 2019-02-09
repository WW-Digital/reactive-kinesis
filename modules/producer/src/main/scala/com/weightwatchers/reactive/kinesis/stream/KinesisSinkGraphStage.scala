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
import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props, Terminated}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.typesafe.scalalogging.LazyLogging
import com.weightwatchers.reactive.kinesis.models.ProducerEvent
import com.weightwatchers.reactive.kinesis.producer.KinesisProducerActor.{
  SendFailed,
  SendSuccessful,
  SendWithCallback
}

import scala.concurrent.{Future, Promise}

/**
  * A KinesisSinkGraph will attach to a kinesis stream with the provided configuration and constitute a Sink[ProducerEvent, Future[Done].
  * This graph stage uses a producer actor to publish events with acknowledgements.
  *
  * @param producerActorProps the properties to create a producer actor where all events are send to. This is a function to work around #48.
  * @param maxOutstanding the number of messages send to the producer which are not acknowledged, before signalling back pressure.
  * @param actorSystem the actor system.
  */
class KinesisSinkGraphStage(producerActorProps: => Props,
                            maxOutstanding: Int,
                            actorSystem: ActorSystem)
    extends GraphStageWithMaterializedValue[SinkShape[ProducerEvent], Future[Done]]
    with LazyLogging {

  private[this] val in: Inlet[ProducerEvent]   = Inlet("KinesisSink.in")
  override def shape: SinkShape[ProducerEvent] = SinkShape.of(in)

  // The materialized value of this graph stage.
  // The promise is fulfilled, when all outstanding messages are acknowledged or the graph stage fails.
  val promise: Promise[Done] = Promise()

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, Future[Done]) = {
    val logic = new GraphStageLogic(shape) {

      // Counts all outstanding messages.
      var outstandingMessageCount: Int = 0

      // The related stage actor.
      implicit var stageActorRef: ActorRef = actorSystem.deadLetters

      // The underlying kinesis producer actor.
      var producerActor: ActorRef = actorSystem.deadLetters

      override def preStart(): Unit = {
        super.preStart()
        // this stage should keep going, even if upstream is finished
        setKeepGoing(true)
        // start up the underlying producer actor
        producerActor = actorSystem.actorOf(producerActorProps)
        // create the stage actor and store a reference
        val stageActor = getStageActor(receive)
        // monitor the producer actor
        stageActor.watch(producerActor)
        stageActorRef = stageActor.ref
        // start the process by signalling demand
        pull(in)
      }

      override def postStop(): Unit = {
        logger.info("Stop Kinesis Sink")
        super.postStop()
        // stop the underlying producer actor
        producerActor ! PoisonPill
        // Finish the promise (it could already be finished in case of failure)
        if (outstandingMessageCount == 0) promise.trySuccess(Done)
        else
          promise.tryFailure(
            new IllegalStateException(s"No acknowledge for $outstandingMessageCount events.")
          )
      }

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val element = grab(in)
            outstandingMessageCount += 1
            producerActor ! SendWithCallback(element)
            if (outstandingMessageCount < maxOutstanding) pull(in)
          }

          override def onUpstreamFinish(): Unit = {
            logger.info("Upstream is finished!")
            // Only finish the stage if there are no outstanding messages
            // If there are outstanding messages, the receive handler will complete the stage.
            if (outstandingMessageCount == 0) completeStage()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            logger.warn(s"Upstream failed: ${ex.getMessage}", ex)
            // signal the failure to the waiting side
            promise.tryFailure(ex)
            super.onUpstreamFailure(ex)
          }
        }
      )

      def receive: Receive = {
        case (_, SendSuccessful(_, recordResult)) =>
          outstandingMessageCount -= 1
          logger.debug(
            s"Message acknowledged: $recordResult. $outstandingMessageCount messages outstanding."
          )
          // upstream is finished?
          if (isClosed(in)) {
            // only complete the stage if all outstanding messages are acknowledged
            if (outstandingMessageCount == 0) completeStage()
          } else {
            // signal demand
            if (outstandingMessageCount < maxOutstanding) pull(in)
          }

        case (_, SendFailed(messageId, reason)) =>
          logger.warn(s"Could not send message with id: $messageId", reason)
          failStage(reason)

        case (_, Terminated(_)) =>
          logger.warn("ProducerActor died unexpectedly.")
          failStage(new IllegalStateException("ProducerActor died unexpectedly."))
      }
    }

    logic -> promise.future
  }
}
