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

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.{ActorContext, ActorRef, ActorRefFactory, ActorSystem, Props}
import akka.util.Timeout
import com.amazonaws.services.kinesis.clientlibrary.config.KinesisClientLibConfigurator
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{
  IRecordProcessor,
  IRecordProcessorFactory
}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{
  KinesisClientLibConfiguration,
  Worker
}
import com.typesafe.config.{Config, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import com.weightwatchers.reactive.kinesis.consumer.CheckpointWorker.CheckpointerConf
import com.weightwatchers.reactive.kinesis.consumer.ConsumerWorker.{ConsumerWorkerConf, _}
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer.ConsumerConf
import com.weightwatchers.reactive.kinesis.utils.TypesafeConfigExtensions

import scala.concurrent.Future
import scala.concurrent.duration.DurationDouble
import scala.util.{Failure, Success, Try}

object KinesisConsumer {

  val KCL_CONFIGURATOR = new KinesisClientLibConfigurator()

  /**
    * CompanionObject object for the [[ConsumerConf]].
    */
  object ConsumerConf {

    /**
      * Given the `kinesisConfig`, builds a combined configuration by taking the `consumerName` specific configuration
      * within, and using the `default-consumer` configuration as a fallback for all values.
      *
      * @param kinesisConfig The top level Kinesis Configuration, containing the specified consumer.
      * @param consumerName  The name of the consumer, which MUST be contained within the `kinesisConfig`
      * @return A [[ConsumerConf]] case class used for constructing the [[KinesisConsumer]]
      */
    def apply(kinesisConfig: Config, consumerName: String): ConsumerConf = {
      import com.weightwatchers.reactive.kinesis.utils.TypesafeConfigExtensions._

      val consumerConfig = kinesisConfig
        .getConfig(consumerName)
        .withFallback(kinesisConfig.getConfig("default-consumer"))

      val streamName = consumerConfig.getString("stream-name")
      require(!streamName.isEmpty,
              "A valid stream name must be provided to start the Kinesis Producer")

      //This represents the table name in dynamo - which MUST be unique per application AND stream
      val applicationName = s"${kinesisConfig.getString("application-name")}-$streamName"
      require(!applicationName.isEmpty,
              "A valid application name must be provided to start the Kinesis Producer")

      val dispatcher: Option[String] =
        if (consumerConfig.getIsNull("akka.dispatcher"))
          None
        else {
          val dispatcherProp = consumerConfig.getString("akka.dispatcher")
          if (dispatcherProp.isEmpty)
            None
          else
            Some(dispatcherProp)
        }

      // Load and modify the KCL config, adding the required properties.
      // Whilst a little hacky, it keeps things consistent with the producer and makes the config structure more manageable.
      val kclConfig = consumerConfig
        .getConfig("kcl")
        .withValue("streamName", ConfigValueFactory.fromAnyRef(streamName))
        .withValue("applicationName", ConfigValueFactory.fromAnyRef(applicationName))

      ConsumerConf(
        KCL_CONFIGURATOR
          .getConfiguration(kclConfig.toProperties), //Convert to java properties to make use of the AWS library
        ConsumerWorkerConf(consumerConfig),
        CheckpointerConf(consumerConfig),
        dispatcher
      )
    }
  }

  /**
    * The collection of configuration values required for constructing a consumer.
    *
    * @param kclConfiguration the AWS KCL Configuration.
    * @param workerConf       the configuration for the worker
    * @param checkpointerConf the configuration for the checkpointer
    * @param dispatcher       an optional dispatcher to be used by this consumer
    */
  final case class ConsumerConf(kclConfiguration: KinesisClientLibConfiguration,
                                workerConf: ConsumerWorkerConf,
                                checkpointerConf: CheckpointerConf,
                                dispatcher: Option[String] = None)

  /**
    * Creates an instance of the [[KinesisConsumer]] along with a ConsumerWorker
    * which will be shared among shards.
    *
    * @param consumerConf   The consumer specific configuration, containing all configuration required for this consumer instance.
    * @param eventProcessor see ConsumerWorker.
    */
  def apply(consumerConf: ConsumerConf,
            eventProcessor: ActorRef,
            context: ActorContext): KinesisConsumer = {

    val workerProps = ConsumerWorker.props(eventProcessor,
                                           consumerConf.workerConf,
                                           consumerConf.checkpointerConf,
                                           consumerConf.dispatcher)

    //Specify the dispatcher according to the config
    new KinesisConsumer(consumerConf,
                        consumerConf.dispatcher.fold(workerProps)(workerProps.withDispatcher),
                        context.system,
                        context)
  }

  /**
    * Creates an instance of the [[KinesisConsumer]] along with a ConsumerWorker
    * which will be shared among shards.
    * - The eventProcessor MUST handle
    *     [[com.weightwatchers.reactive.kinesis.consumer.ConsumerWorker.ProcessEvent]] messages (for each message)
    * - The eventProcesser MUST respond with [[com.weightwatchers.reactive.kinesis.consumer.ConsumerWorker.EventProcessed]] after
    *     processing of the [[com.weightwatchers.reactive.kinesis.consumer.ConsumerWorker.ProcessEvent]]
    * - The eventProcessor may set `successful` to false to indicate the message can be skipped
    * - The eventProcesser SHOULD handle [[com.weightwatchers.reactive.kinesis.consumer.ConsumerWorker.ConsumerWorkerFailure]]
    *     messages which signal a critical failure in the Consumer.
    * - The eventProcessor SHOULD handle [[com.weightwatchers.reactive.kinesis.consumer.ConsumerWorker.ConsumerShutdown]]
    *     messages which siganl a graceful shutdown of the Consumer.
    *
    * @param consumerConf   The consumer specific configuration, containing all configuration required for this consumer instance.
    * @param eventProcessor see ConsumerWorker for more information.
    */
  def apply(consumerConf: ConsumerConf,
            eventProcessor: ActorRef,
            system: ActorSystem): KinesisConsumer = {

    val workerProps = ConsumerWorker.props(eventProcessor,
                                           consumerConf.workerConf,
                                           consumerConf.checkpointerConf,
                                           consumerConf.dispatcher)

    //Specify the dispatcher according to the config
    new KinesisConsumer(consumerConf,
                        consumerConf.dispatcher.fold(workerProps)(workerProps.withDispatcher),
                        system,
                        system)
  }
}

/**
  * A Kinesis consumer which wraps Amazon's KCL and performs reliable asynchronous checkpointing.
  *
  * NOTE: This should be created via the companion object which will create the worker.
  *
  * @param consumerConf        The consumer specific configuration, containing all configuration required
  *                            for this consumer instance.
  * @param consumerWorkerProps the worker props for processing requests and handling all checkpointing.
  * @param system              This is required to lookup the dispatchers and create the actors.
  *                            We specifically need a system for this purpose.
  * @param context             This will be used to create the actor hierarchy.
  *                            So all Actors created will be children/grandchildren of this context.
  *                            This can be the same value as the `system` but we don't want to force the user
  *                            into using an ActorSystem vs ActorContext.
  *
  */
class KinesisConsumer(consumerConf: ConsumerConf,
                      consumerWorkerProps: Props,
                      system: ActorSystem,
                      context: ActorRefFactory)
    extends LazyLogging {

  //The manager timeout needs to be just longer than the batch timeout * retries
  val managerBatchTimeout = Timeout(
    (consumerConf.workerConf.batchTimeout.toMillis
    * (consumerConf.workerConf.failedMessageRetries + 1.25)).millis
  )

  val isShuttingDown = new AtomicBoolean(false)

  implicit val dispatcher =
    consumerConf.dispatcher.fold(system.dispatcher)(system.dispatchers.lookup)

  private[consumer] val recordProcessorFactory: IRecordProcessorFactory =
    new IRecordProcessorFactory {

      /**
        * Creates an instance of [[ConsumerProcessingManager]].
        * Passing a newly created ConsumerWorker Actor specific to this shard (and manager).
        */
      override def createProcessor(): IRecordProcessor = {
        logger.debug(s"Creating ConsumerWorker: ${consumerWorkerProps.args}")

        //TODO define supervisor
        //multiply timeout to ensure worker always times out first
        new ConsumerProcessingManager(
          context.actorOf(consumerWorkerProps, s"consumer-worker-${UUID_GENERATOR.generate()}"),
          kclWorker,
          managerBatchTimeout,
          consumerConf.workerConf.shutdownTimeout.duration
        )
      }
    }

  lazy val kclWorker: Worker = new Worker.Builder()
    .recordProcessorFactory(recordProcessorFactory)
    .config(consumerConf.kclConfiguration)
    .build()

  // Ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
  java.security.Security.setProperty("networkaddress.cache.ttl", "60")

  /**
    * The Future returned is long running, completion of the future indicates we're no
    * longer processing messages and should be handled accordingly by the callee.
    */
  def start(): Future[Unit] = {
    logger.info(s"""
         |-----------------------------------------------------------------------
         |-------------------- Initialising Kinesis Consumer --------------------
         |-----------------------------------------------------------------------
         |***** Running ${consumerConf.kclConfiguration.getApplicationName} *****
         |***** Processing Stream: ${consumerConf.kclConfiguration.getStreamName} *****
         |***** WorkerId: ${consumerConf.kclConfiguration.getWorkerIdentifier} *****
         |-----------------------------------------------------------------------
     """.stripMargin)

    if (consumerConf.workerConf.shutdownHook) {
      // Adding the shutdown hook for shutting down consumer
      sys.addShutdownHook({
        stop()
      })
    }

    Future {
      kclWorker.run()
      logger.info(
        s"*** Kinesis consumer for Stream ${consumerConf.kclConfiguration.getStreamName} completed ***"
      )
    } recoverWith {
      case t: Throwable =>
        logger.error(
          s"*** Caught throwable while processing data from Kinesis Stream: ${consumerConf.kclConfiguration.getStreamName} ***",
          t
        )
        Future.failed(t)
    }
  }

  /**
    * Gracefully Shutdown this Consumer.
    */
  def stop(): Unit = {
    val canShutdown = isShuttingDown.compareAndSet(false, true)
    if (canShutdown) {
      logger.info(
        s"*** Shutting down Kinesis Consumer for Stream ${consumerConf.kclConfiguration.getStreamName} ***"
      )

      val shutdownTimeoutLength = consumerConf.workerConf.shutdownTimeout.duration.length
      val shutdownTimeoutUnit   = consumerConf.workerConf.shutdownTimeout.duration.unit

      Try {
        kclWorker
          .startGracefulShutdown()
          .get(shutdownTimeoutLength, shutdownTimeoutUnit)
      } match {
        case Success(_) =>
          logger.info(
            s"*** Shutdown for Kinesis Consumer for Stream ${consumerConf.kclConfiguration.getStreamName} completed ***"
          )
        case Failure(ex) =>
          logger.error(
            s"*** Shutdown failed for Kinesis Consumer for Stream ${consumerConf.kclConfiguration.getStreamName} ***",
            ex
          )
      }
    } else {

      logger.warn(
        s"*** Shutdown attempted twice for Kinesis Consumer for Stream ${consumerConf.kclConfiguration.getStreamName} ***"
      )
    }
  }
}
