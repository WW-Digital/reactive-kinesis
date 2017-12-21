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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer.ConsumerConf

/**
  * Main entry point for creating a Kinesis source and sink.
  */
object Kinesis {

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
  )(implicit system: ActorSystem): Source[KinesisEvent, NotUsed] = {
    Source.fromGraph(new KinesisSourceGraph(consumerConf, system))
  }

  /**
    * Create a source by using the actor system configuration, that provides KinesisEvents.
    * Please note: every KinesisEvent has to be committed during the user flow!
    * Uncommitted events will be retransmitted after a timeout.
    *
    * The application conf file should look like this:
    * {{{
    * kinesis {
    *    application-name = "SampleService"
    *    consumer-name {
    *       stream-name = "sample-consumer"
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
  def source(consumerName: String, inConfig: String = "kinesis")(
      implicit system: ActorSystem
  ): Source[KinesisEvent, NotUsed] = {
    source(ConsumerConf(system.settings.config.getConfig(inConfig), consumerName))
  }
}
