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
import com.weightwatchers.reactive.kinesis.consumer.{ConsumerService, KinesisConsumer}
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer.ConsumerConf
import com.weightwatchers.reactive.kinesis.models.{ConsumerEvent, ProducerEvent}

import scala.concurrent.Future

/**
  * Main entry point for creating a Kinesis source and sink.
  */
object ConsumerStreamFactory extends LazyLogging {

  /**
    * Create a source, that provides KinesisEvents.
    * Please note: every KinesisEvent has to be committed during the user flow!
    * Uncommitted events will be retransmitted after a timeout.
    *
    * @param consumerConf the configuration to connect to Kinesis.
    * @param createConsumer factory function to create ConsumerService from eventProcessor ActorRef.
    * @param system the actor system.
    * @return A source of KinesisEvent objects.
    */
  def source(
      consumerConf: ConsumerConf,
      createConsumer: ActorRef => ConsumerService
  )(implicit system: ActorSystem): Source[CommittableEvent[ConsumerEvent], NotUsed] = {
    Source.fromGraph(new KinesisSourceGraphStage(consumerConf, createConsumer, system))
  }

  /**
    * Create a source, that provides KinesisEvents.
    * Please note: every KinesisEvent has to be committed during the user flow!
    * Uncommitted events will be retransmitted after a timeout.
    *
    * @param consumerConf the configuration to connect to Kinesis.
    * @param system the actor system.
    * @return A source of KinesisEvent objects.
    */
  def source(
      consumerConf: ConsumerConf
  )(implicit system: ActorSystem): Source[CommittableEvent[ConsumerEvent], NotUsed] = {
    source(consumerConf, KinesisConsumer(consumerConf, _, system))
  }

  /**
    * Create a source by using the actor system configuration, that provides KinesisEvents.
    * Please note: every KinesisEvent has to be committed during the user flow!
    * Uncommitted events will be retransmitted after a timeout.
    *
    * A minimal application conf file should look like this:
    * {{{
    * kinesis {
    *    application-name = "SampleService"
    *    consumer-name {
    *       stream-name = "sample-stream"
    *    }
    * }
    * }}}
    * See kinesis reference.conf for a list of all available config options.
    *
    * @param consumerName the name of the consumer in the application.conf.
    * @param inConfig the name of the sub-config for kinesis.
    * @param system the actor system to use.
    * @return A source of KinesisEvent objects.
    */
  def source(consumerName: String, inConfig: String)(
      implicit system: ActorSystem
  ): Source[CommittableEvent[ConsumerEvent], NotUsed] = {
    source(ConsumerConf(system.settings.config.getConfig(inConfig), consumerName))
  }

  /**
    * Create a source by using the actor system configuration, that provides KinesisEvents.
    * Please note: every KinesisEvent has to be committed during the user flow!
    * Uncommitted events will be retransmitted after a timeout.
    *
    * A minimal application conf file should look like this:
    * {{{
    * kinesis {
    *    application-name = "SampleService"
    *    consumer-name {
    *       stream-name = "sample-stream"
    *    }
    * }
    * }}}
    * See kinesis reference.conf for a list of all available config options.
    *
    * @param consumerName the name of the consumer in the application.conf.
    * @param system the actor system to use.
    * @return A source of KinesisEvent objects.
    */
  def source(consumerName: String)(
      implicit system: ActorSystem
  ): Source[CommittableEvent[ConsumerEvent], NotUsed] = {
    source(consumerName, "kinesis")
  }

  /**
    * Create a source by using the actor system configuration, that provides KinesisEvents.
    * Please note: every KinesisEvent has to be committed during the user flow!
    * Uncommitted events will be retransmitted after a timeout.
    *
    * A minimal application conf file should look like this:
    * {{{
    * kinesis {
    *    application-name = "SampleService"
    *    consumer-name {
    *       stream-name = "sample-stream"
    *    }
    * }
    * }}}
    * See kinesis reference.conf for a list of all available config options.
    *
    * @param consumerName the name of the consumer in the application.conf.
    * @param createConsumer factory function to create ConsumerService from eventProcessor ActorRef.
    * @param inConfig the name of the sub-config for kinesis.
    * @param system the actor system to use.
    * @return A source of KinesisEvent objects.
    */
  def source(
      consumerName: String,
      createConsumer: (ConsumerConf, ActorRef) => ConsumerService,
      inConfig: String = "kinesis"
  )(implicit system: ActorSystem): Source[CommittableEvent[ConsumerEvent], NotUsed] = {
    val consumerConf = ConsumerConf(system.settings.config.getConfig(inConfig), consumerName)
    source(consumerConf, createConsumer(consumerConf, _))
  }

}
