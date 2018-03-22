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

package com.weightwatchers.reactive.kinesis.producer

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.kinesis.producer.{
  KinesisProducerConfiguration,
  UserRecordResult,
  KinesisProducer => AWSKinesisProducer
}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.weightwatchers.reactive.kinesis.models.ProducerEvent
import com.weightwatchers.reactive.kinesis.utils.{FutureUtils, TypesafeConfigExtensions}

import scala.concurrent.{ExecutionContextExecutor, Future}

object KinesisProducer extends LazyLogging {

  /**
    * The config passed is expected to contain the AWS KPL properties at the top level.
    *
    * @see `src/it/resources/reference.conf` for a more detailed example.
    * @param kplConfig           The KPL configuration properties, at the top level. This config can be reused by multiple producers.
    *                            We expect body of the "kpl" section of that config to be passed here (e.g. kinesisConfig.getConfig("kpl).
    *                            This set of properties matches the official Kinesis KPL properties:
    *                            http://docs.aws.amazon.com/streams/latest/dev/kinesis-kpl-config.html
    * @param streamName          Th name of the Kinesis stream, which must exist.
    * @param credentialsProvider A specific CredentialsProvider. The KCL defaults to DefaultAWSCredentialsProviderChain.
    * @return an instantiated [[KinesisProducer]]
    */
  @deprecated("Use KinesisProducer(producerConf: ProducerConf) instead", "v0.5.7")
  def apply(kplConfig: Config,
            streamName: String,
            credentialsProvider: Option[AWSCredentialsProvider] = None): KinesisProducer = {

    import TypesafeConfigExtensions._

    // We directly load our properties into the KPL as a Java `Properties` object
    // See http://docs.aws.amazon.com/streams/latest/dev/kinesis-kpl-config.html
    val kplProps = kplConfig.toProperties

    logger.debug(
      s"Creating Kinesis Producer for Stream $streamName with the following settings ${kplProps.toString}"
    )

    val kplLibConfiguration: KinesisProducerConfiguration =
      KinesisProducerConfiguration.fromProperties(kplProps)
    credentialsProvider.foreach(kplLibConfiguration.setCredentialsProvider)

    new KinesisProducer(new AWSKinesisProducer(kplLibConfiguration), streamName)
  }

  /**
    * The config passed is expected to contain the AWS KPL properties at the top level.
    *
    * @param producerConf An instance of [[ProducerConf]] which contains all required configuration for the KPL.
    * @return an instantiated [[KinesisProducer]]
    */
  def apply(producerConf: ProducerConf): KinesisProducer = {
    apply(producerConf.kplLibConfiguration, producerConf.streamName)
  }

  /**
    * The [[KinesisProducerConfiguration]] argument is passed directly to the KPL library.
    * This constructor makes no use of the Typesafe config.
    *
    * @see `src/it/resources/reference.conf` for a more detailed example.
    * @param kplConfig  An instance of the underlying [[KinesisProducerConfiguration]] to be passed
    *                   directly to the library.
    * @param streamName Th name of the Kinesis stream, which must exist.
    * @return an instantiated [[KinesisProducer]]
    */
  def apply(kplConfig: KinesisProducerConfiguration, streamName: String): KinesisProducer = {
    //TODO add logging
    new KinesisProducer(new AWSKinesisProducer(kplConfig), streamName)
  }
}

/**
  * Sends messages to the configured Kinesis stream.
  *
  * To create an instance of this class, we recommend using the apply method to instantiate from config.
  */
class KinesisProducer(kinesis: AWSKinesisProducer, streamName: String) extends LazyLogging {

  val underlying         = kinesis
  private var _destroyed = false

  //TODO rather than validating the stream at the start, `addUserRecord` will return a Failure
  //TODO seems difficult to get access to stream specific operations from producer

  /**
    * Adds a message to the next batch to be sent to the configured stream.
    *
    * @return On success: Future{UserRecordResult}
    *         On failure: Future.failed(...): Any Throwable related to put.
    * @see Callee `com.amazonaws.services.kinesis.producer.KinesisProducer.addUserRecord`
    * @see UserRecordResult
    * @see KinesisProducerConfiguration#setRecordTtl(long)
    * @see UserRecordFailedException
    */
  def addUserRecord(
      event: ProducerEvent
  )(implicit ec: ExecutionContextExecutor): Future[UserRecordResult] = {
    assert(!_destroyed, "Kinesis has been destroyed, no longer accepting messages") //TODO specific exception?
    import FutureUtils._
    kinesis.addUserRecord(streamName, event.partitionKey, event.payload).asScalaFuture
  }

  /**
    * Get the number of unfinished records currently being processed. The
    * records could either be waiting to be sent to the child process, or have
    * reached the child process and are being worked on.
    *
    * <p>
    * This is equal to the number of futures returned from [[addUserRecord]]
    * that have not finished.
    *
    * This is useful for applying backpressure and throttling the number of concurrent Futures.
    *
    * @return The number of unfinished records currently being processed.
    */
  def outstandingRecordsCount(): Int = {
    kinesis.getOutstandingRecordsCount
  }

  /**
    * Firstly, blocks whilst all all records are complete (either succeeding or failing).
    *
    * <p>
    *
    * The includes whilst any retries are performed. Depending on
    * your configuration of record TTL and request timeout, this can
    * potentially take a long time if the library is having trouble delivering
    * records to the backend, for example due to network problems.
    *
    * <p>
    *
    * Finally the [[KinesisProducer]] is destroyed, preventing further use.
    *
    * @throws com.amazonaws.services.kinesis.producer.DaemonException if the child process is dead //TODO - handle this better?
    * @see [[AWSKinesisProducer]]
    */
  def stop(): Unit = {
    kinesis.flushSync() //This blocks until all records are flushed
    kinesis.destroy()
    _destroyed = true
  }

  /**
    * @return true if the [[KinesisProducer]] has been stopped & destroyed.
    */
  def destroyed(): Boolean = _destroyed
}
