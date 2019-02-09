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

import akka.{Done, NotUsed}
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.scaladsl.{Sink, Source}
import com.amazonaws.auth.AWSCredentialsProvider
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.weightwatchers.reactive.kinesis.models.{ConsumerEvent, ProducerEvent}
import com.weightwatchers.reactive.kinesis.producer.{KinesisProducerActor, ProducerConf}

import scala.concurrent.Future

/**
  * Main entry point for creating a Kinesis source and sink.
  */
object Kinesis extends LazyLogging {

  /**
    * Create a Sink that accepts ProducerEvents, which get published to Kinesis.
    *
    * The sink itself sends all events to an actor, which is created with the given Props.
    * Every message send needs to be acknowledged by the underlying producer actor.
    *
    * This sink signals back pressure, if more than maxOutstanding messages are not acknowledged.
    *
    * The sink produces a materialized value `Future[Done]`, which is finished if all messages of the stream are send to the producer actor _and_ got acknowledged.
    * The future fails, if the sending an event fails or upstream has failed the stream.
    *
    * @param props the props to create a producer actor. This is a function to work around #48.
    * @param maxOutStanding the number of messages to send to the actor unacknowledged before back pressure is applied.
    * @param system the actor system.
    * @return A sink that accepts ProducerEvents.
    */
  def sink(
      props: => Props,
      maxOutStanding: Int
  )(implicit system: ActorSystem): Sink[ProducerEvent, Future[Done]] = {
    Sink.fromGraph(new KinesisSinkGraphStage(props, maxOutStanding, system))
  }

  /**
    * Create a Sink that accepts ProducerEvents, which get published to Kinesis.
    *
    * The sink itself sends all events to an KinesisProducerActor which is configured with given config object.
    * Every message send needs to be acknowledged by the underlying producer actor.
    *
    * This sink signals back pressure, if more messages than configured in throttling conf are not acknowledged.
    * If throttling is not configured, a default value (= 1000 messages) is applied.
    *
    * The sink produces a materialized value `Future[Done]`, which is finished if all messages of the stream are send to the producer actor _and_ got acknowledged.
    * The future fails, if the sending an event fails or upstream has failed the stream.
    *
    * @param producerConf the configuration to create KinesisProducerActor
    * @param system the actor system.
    * @return A sink that accepts ProducerEvents.
    */
  def sink(
      producerConf: ProducerConf
  )(implicit system: ActorSystem): Sink[ProducerEvent, Future[Done]] = {
    val maxOutstanding = producerConf.throttlingConf.fold {
      logger.info(
        "Producer throttling not configured - set maxOutstanding to 1000. Configure with: kinesis.{producer}.akka.max-outstanding-requests=1000"
      )
      1000
    }(_.maxOutstandingRequests)
    sink(KinesisProducerActor.props(producerConf), maxOutstanding)
  }

  /**
    * Create a Sink that accepts ProducerEvents, which get published to Kinesis.
    *
    * The sink itself sends all events to an KinesisProducerActor which is configured from the system configuration for given producer name.
    * Every message send needs to be acknowledged by the underlying producer actor.
    *
    * This sink signals back pressure, if more messages than configured in throttling conf are not acknowledged.
    * If throttling is not configured, a default value (= 1000 messages) is applied.
    *
    * The sink produces a materialized value `Future[Done]`, which is finished if all messages of the stream are send to the producer actor _and_ got acknowledged.
    * The future fails, if the sending an event fails or upstream has failed the stream.
    *
    * @param kinesisConfig the configuration object that holds the producer config.
    * @param producerName the name of the producer in the system configuration.
    * @param credentialsProvider the AWS credentials provider to use to connect.
    * @param system the actor system.
    * @return A sink that accepts ProducerEvents.
    */
  def sink(
      kinesisConfig: Config,
      producerName: String,
      credentialsProvider: Option[AWSCredentialsProvider]
  )(implicit system: ActorSystem): Sink[ProducerEvent, Future[Done]] = {
    sink(
      ProducerConf(kinesisConfig, producerName, credentialsProvider)
    )
  }

  /**
    * Create a Sink that accepts ProducerEvents, which get published to Kinesis.
    *
    * The sink itself sends all events to an KinesisProducerActor which is configured from the system configuration for given producer name.
    * Every message send needs to be acknowledged by the underlying producer actor.
    *
    * This sink signals back pressure, if more messages than configured in throttling conf are not acknowledged.
    * If throttling is not configured, a default value (= 1000 messages) is applied.
    *
    * The sink produces a materialized value `Future[Done]`, which is finished if all messages of the stream are send to the producer actor _and_ got acknowledged.
    * The future fails, if the sending an event fails or upstream has failed the stream.
    *
    * A minimal application conf file should look like this:
    * {{{
    * kinesis {
    *    application-name = "SampleService"
    *    producer-name {
    *       stream-name = "sample-stream"
    *       akka.max-outstanding-requests = 100
    *    }
    * }
    * }}}
    * See kinesis reference.conf for a list of all available config options.
    *
    * @param producerName the name of the producer in the system configuration.
    * @param inConfig the configuration object that holds the producer config (usually kinesis).
    * @param credentialsProvider the AWS credentials provider to use to connect.
    * @param system the actor system.
    * @return A sink that accepts ProducerEvents.
    */
  def sink(
      producerName: String,
      inConfig: String = "kinesis",
      credentialsProvider: Option[AWSCredentialsProvider] = None
  )(implicit system: ActorSystem): Sink[ProducerEvent, Future[Done]] = {
    sink(system.settings.config.getConfig(inConfig), producerName, credentialsProvider)
  }
}
