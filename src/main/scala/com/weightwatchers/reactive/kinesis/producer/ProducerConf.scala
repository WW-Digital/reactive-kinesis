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

package com.weightwatchers.reactive.kinesis.producer

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration.ThreadingModel
import com.typesafe.config.Config
import com.weightwatchers.reactive.kinesis.producer.ProducerConf.ThrottlingConf
import com.weightwatchers.reactive.kinesis.utils.TypesafeConfigExtensions

import scala.concurrent.duration.{FiniteDuration, _}

/**
  * Companion object for the [[ProducerConf]].
  */
object ProducerConf {

  /**
    * Configuration which defines whether and how often to throttle.
    * Only applicable when using the actor interface.
    *
    * @param maxOutstandingRequests The max number of concurrent requests before throttling. None removes throttling completely.
    * @param retryDuration          The time before retrying after throttling.
    */
  final case class ThrottlingConf(maxOutstandingRequests: Int,
                                  retryDuration: FiniteDuration = 100.millis)

  /**
    * Given the top level `kinesis` config block, builds a combined configuration by taking the `producerName` specific configuration
    * within, and using the `default-producer` configuration as a fallback for all values.
    *
    * @see `src/it/resources/reference.conf` for a more detailed example of the KinesisConfig.
    * @param kinesisConfig       The top level Kinesis Configuration, containing the specified producer.
    * @param producerName        The name of the producer, which MUST be contained within the `kinesisConfig`
    * @param credentialsProvider A specific CredentialsProvider. The KPL defaults to [[com.amazonaws.auth.DefaultAWSCredentialsProviderChain]].
    * @return A [[ProducerConf]] case class used for constructing the [[KinesisProducerActor]]
    */
  def apply(kinesisConfig: Config,
            producerName: String,
            credentialsProvider: Option[AWSCredentialsProvider] = None): ProducerConf = {

    val producerConfig = kinesisConfig
      .getConfig(producerName)
      .withFallback(kinesisConfig.getConfig("default-producer"))

    val streamName = producerConfig.getString("stream-name")
    require(
      !streamName.isEmpty,
      s"Config field `stream-name` missing, a value must be provided to start the Kinesis Producer!"
    )

    val dispatcher: Option[String] =
      if (producerConfig.getIsNull("akka.dispatcher"))
        None
      else {
        val dispatcherProp = producerConfig.getString("akka.dispatcher")
        if (dispatcherProp.isEmpty)
          None
        else
          Some(dispatcherProp)
      }

    val kplConfig = producerConfig.getConfig("kpl")
    val kplLibConfiguration: KinesisProducerConfiguration =
      buildKPLConfig(kplConfig, credentialsProvider)

    new ProducerConf(streamName,
                     kplLibConfiguration,
                     dispatcher,
                     parseThrottlingConfig(producerConfig))
  }

  /**
    * Simple typed apply method
    *
    * @param kinesisConfig     The top level Kinesis Configuration, containing the specified producer
    * @param streamName        The name of the Kinesis stream to consume
    * @param dispatcher        Optional config path for the akka dispatcher
    * @param throttlingConf    Optional configuration which defines whether and how often to throttle
    * @return A [[ProducerConf]] case class used for constructing the [[KinesisProducerActor]]
    */
  def apply(kinesisConfig: KinesisProducerConfig,
            streamName: String,
            dispatcher: Option[String],
            throttlingConf: Option[ThrottlingConf]): ProducerConf = {

    new ProducerConf(streamName, kinesisConfig.toAwsConfig, dispatcher, throttlingConf)
  }

  def buildKPLConfig(kplConfig: Config, credentialsProvider: Option[AWSCredentialsProvider]) = {
    // We directly load our properties into the KPL as a Java `Properties` object
    // See http://docs.aws.amazon.com/streams/latest/dev/kinesis-kpl-config.html
    import TypesafeConfigExtensions._
    val kplProps = kplConfig.toProperties

    val kplLibConfiguration: KinesisProducerConfiguration =
      KinesisProducerConfiguration.fromProperties(kplProps)
    credentialsProvider.foreach(kplLibConfiguration.setCredentialsProvider)

    kplLibConfiguration
  }

  private def parseThrottlingConfig(producerConfig: Config): Option[ThrottlingConf] = {
    if (!producerConfig.hasPath("akka.max-outstanding-requests")
        || producerConfig.getIsNull("akka.max-outstanding-requests"))
      None
    else {
      val maxOutstandingRequests = producerConfig.getInt("akka.max-outstanding-requests")

      if (!producerConfig.hasPath("akka.throttling-retry-millis")
          || producerConfig.getIsNull("akka.throttling-retry-millis"))
        Some(ThrottlingConf(maxOutstandingRequests))
      else
        Some(
          ThrottlingConf(maxOutstandingRequests,
                         producerConfig.getLong("akka.throttling-retry-millis").millis)
        )
    }
  }

}

/**
  * The collection of configuration values required for constructing a producer. See the companion object.
  *
  * @param streamName          The name of the Kinesis Stream this producer will publish to.
  * @param kplLibConfiguration An instance of the underlying [[KinesisProducerConfiguration]] for the KPL library.
  * @param dispatcher          An optional dispatcher for the producer and kpl.
  * @param throttlingConf      Configuration which defines whether and how often to throttle.
  */
final case class ProducerConf(streamName: String,
                              kplLibConfiguration: KinesisProducerConfiguration,
                              dispatcher: Option[String],
                              throttlingConf: Option[ThrottlingConf])
