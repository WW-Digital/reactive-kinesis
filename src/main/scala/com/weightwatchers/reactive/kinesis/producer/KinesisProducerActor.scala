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

import akka.actor.{Actor, Cancellable, Props, UnboundedStash}
import akka.event.LoggingReceive
import akka.pattern._
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.kinesis.producer.{UserRecordFailedException, UserRecordResult}
import com.fasterxml.uuid.Generators
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.weightwatchers.reactive.kinesis.models.ProducerEvent
import com.weightwatchers.reactive.kinesis.producer.KinesisProducerActor._

import scala.concurrent.Future
import scala.concurrent.duration._

object KinesisProducerActor {

  /**
    * Compainion object for the [[ProducerConf]].
    */
  object ProducerConf {

    /**
      * Given the `kinesisConfig`, builds a combined configuration by taking the `producerName` specific configuration
      * within, and using the `default-producer` configuration as a fallback for all values.
      *
      * @param kinesisConfig The top level Kinesis Configuration, containing the specified producer.
      * @param producerName  The name of the producer, which MUST be contained within the `kinesisConfig`
      * @return A [[ProducerConf]] case class used for constructing the [[KinesisProducerActor]]
      */
    def apply(kinesisConfig: Config, producerName: String): ProducerConf = {

      val producerConfig = kinesisConfig
        .getConfig(producerName)
        .withFallback(kinesisConfig.getConfig("default-producer"))

      val streamName = producerConfig.getString("stream-name")
      require(!streamName.isEmpty,
              "A valid stream name must be provided to start the Kinesis Producer")

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

      new ProducerConf(streamName,
                       producerConfig.getConfig("kpl"),
                       dispatcher,
                       parseThrottlingConfig(producerConfig))
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
    * @param streamName     The name of the Kinesis Stream this producer will publish to.
    * @param kplConfig      The `kpl` section of the kinesis configuration.
    * @param dispatcher     An optional dispatcher for the producer and kpl.
    * @param throttlingConf Configuration which defines whether and how often to throttle.
    */
  final case class ProducerConf(streamName: String,
                                kplConfig: Config,
                                dispatcher: Option[String] = None,
                                throttlingConf: Option[ThrottlingConf] = None)

  /**
    * Configuration which defines whether and how often to throttle.
    *
    * @param maxOutstandingRequests The max number of concurrent requests before throttling. None removes throttling completely.
    * @param retryDuration          The time before retrying after throttling.
    */
  protected final case class ThrottlingConf(maxOutstandingRequests: Int,
                                            retryDuration: FiniteDuration = 100.millis)

  private val UUID_GENERATOR = Generators.timeBasedGenerator()

  /**
    * Send a message to Kinesis, registering a callback response of [[SendSuccessful]] or [[SendFailed]] accordingly.
    */
  case class SendWithCallback(producerEvent: ProducerEvent,
                              messageId: String = UUID_GENERATOR.generate().toString)

  /**
    * Send a message to Kinesis witout any callbacks. Fire and forget.
    */
  case class Send(producerEvent: ProducerEvent)

  /**
    * Sent to the sender in event of a successful completion.
    *
    * @param messageId        The id of the event that was sent.
    * @param userRecordResult The Kinesis data regarding the send.
    */
  case class SendSuccessful(messageId: String, userRecordResult: UserRecordResult)

  /**
    * Sent to the sender in event of a failed completion.
    *
    * @param messageId The id of the event that failed.
    * @param reason    The exception causing the failure.
    *                  Likely to be of type [[com.amazonaws.services.kinesis.producer.UserRecordFailedException]]
    */
  case class SendFailed(messageId: String, reason: Throwable)

  private case object UnThrottle

  /**
    * Create a [[KinesisProducerKPL]] and passes it to a [[KinesisProducerActor]], returning the Props.
    *
    * This function will attempt to load config (per value) from the `producerName` section within `kinesisConfig`.
    *
    * Values from the `default-producer` section will be used for any missing configurations.
    *
    * `stream-name` MUST be specified within the producer specific configuration.
    *
    * @see `src/main/resources/reference.conf` for the default configuration.
    * @see `src/it/resources/application.conf` for a override configuration example.
    * @param kinesisConfig       The top level Kinesis configuration. This MUST contain the producer configuration (as per the name)
    *                            in addition to the `aws` configuration.
    * @param producerName        The name of the producer, as per the configuration.
    * @param credentialsProvider A specific CredentialsProvider. The KCL defaults to DefaultAWSCredentialsProviderChain.
    */
  def props(kinesisConfig: Config,
            producerName: String,
            credentialsProvider: Option[AWSCredentialsProvider] = None): Props = {
    val producerConf = ProducerConf(kinesisConfig, producerName)
    props(producerConf, credentialsProvider)
  }

  /**
    * Create a [[KinesisProducerKPL]] and passes it to a [[KinesisProducerActor]], returning the Props.
    *
    * @param producerConf        A complete [[ProducerConf]] case class.
    * @param credentialsProvider A specific CredentialsProvider. The KCL defaults to DefaultAWSCredentialsProviderChain.
    */
  def props(producerConf: ProducerConf,
            credentialsProvider: Option[AWSCredentialsProvider]): Props = {
    val kinesisProducer =
      KinesisProducerKPL(producerConf.kplConfig, producerConf.streamName, credentialsProvider)

    val props = Props(classOf[KinesisProducerActor], kinesisProducer, producerConf.throttlingConf)
    producerConf.dispatcher.fold(props)(props.withDispatcher)
  }

  def props(kinesisProducer: KinesisProducer, maxOutstandingRequests: Int): Props =
    Props(classOf[KinesisProducerActor],
          kinesisProducer,
          Some(ThrottlingConf(maxOutstandingRequests)))

  def props(kinesisProducer: KinesisProducer): Props =
    Props(classOf[KinesisProducerActor], kinesisProducer, None)
}

/**
  * This ``Actor`` wraps the [[KinesisProducerKPL]] to provide reliable handling and throttling of requests.
  *
  * Upon completion of a [[com.weightwatchers.reactive.kinesis.producer.KinesisProducerActor.SendWithCallback]],
  * a [[com.weightwatchers.reactive.kinesis.producer.KinesisProducerActor.SendSuccessful]]
  * or [[com.weightwatchers.reactive.kinesis.producer.KinesisProducerActor.SendFailed]] will be returned to the original sender,
  * this allows asynchronous tracking of requests.
  *
  * Internally, for each request a new Future is created to track the completion. These concurrently created Futures
  * can be throttled by specifying the maxOutstandingRequests.
  *
  * This causes subsequent requests to be queued up until the outstanding messages have been processed.
  *
  * @param producer         an instance of the [[KinesisProducer]]
  * @param throttlingConfig Configuration which defines whether and how often to throttle.
  */
class KinesisProducerActor(producer: KinesisProducer, throttlingConfig: Option[ThrottlingConf])
    extends Actor
    with LazyLogging
    with UnboundedStash {

  import context.dispatcher

  private val UNTHROTTLE_THRESHOLD = 0.9

  private var throttlingRetryTimer: Option[Cancellable] = None

  override def receive: Receive = processing

  private def throttling: Receive = LoggingReceive {
    // TODO should we have a different threshold for unthrottling?
    case UnThrottle if unthrottle =>
      logger.warn("Removing Throttling for sends to Kinesis.")
      context.become(processing)
      unstashAll()

    case UnThrottle =>
      restartNotificationTimer()

    case _ => stash()
  }

  private def processing: Receive = LoggingReceive {
    case _ if throttle =>
      //TODO should we send some sort of notification to the sender to indicate throttling?
      logger.info(
        s"Reached maxOutstandingRequests threshold ($throttlingConfig). Throttling sends to Kinesis."
      )
      stash()
      context.become(throttling)
      restartNotificationTimer()

    case SendWithCallback(event, messageId) => {

      val result: Future[UserRecordResult] = producer.addUserRecord(event)

      result
        .map { result =>
          logger.trace(s"Succesfully sent message to kinesis: $event")
          SendSuccessful(messageId, result)
        }
        .recover {
          case ex: UserRecordFailedException =>
            //TODO is this too much log output on error? I'm assuming this will be rare!
            import scala.collection.JavaConverters._
            val errorList = ex.getResult.getAttempts.asScala.map(attempt => s"""
               |Delay after prev attempt: ${attempt.getDelay} ms,
               |Duration: ${attempt.getDuration} ms, Code: ${attempt.getErrorCode},
               |Message: ${attempt.getErrorMessage}
            """.stripMargin)
            logger.warn(
              s"Record failed to put, partitionKey=${event.partitionKey}, payload=${event.payload}, attempts:$errorList",
              ex
            )
            SendFailed(messageId, ex)
          case ex =>
            logger.warn(s"Failed to send message to kinesis with: $event", ex)
            SendFailed(messageId, ex)
        }
        .pipeTo(sender)
    }

    case Send(event) =>
      producer.addUserRecord(event)

    case msg => logger.debug(s"KinesisProducerActor received unexpected message $msg")
  }

  private def throttle: Boolean =
    throttlingConfig.exists(producer.outstandingRecordsCount() >= _.maxOutstandingRequests)

  private def unthrottle: Boolean =
    throttlingConfig.exists(
      producer.outstandingRecordsCount() < _.maxOutstandingRequests * UNTHROTTLE_THRESHOLD
    )

  private def restartNotificationTimer(): Unit = {
    throttlingRetryTimer.map(_.cancel())
    throttlingRetryTimer = Some(
      context.system.scheduler
        .scheduleOnce(throttlingConfig.get.retryDuration, self, UnThrottle)(context.dispatcher)
    )
  }

  override def aroundPostStop(): Unit = {
    //TODO what if we're currently throttled and have messages stashed??
    producer.stop()
  }
}
