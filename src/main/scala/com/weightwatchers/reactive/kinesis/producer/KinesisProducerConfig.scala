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
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration.ThreadingModel
import com.amazonaws.services.kinesis.producer.protobuf.Config.AdditionalDimension

/** Typed config class for the KPL */
final case class KinesisProducerConfig(
    additionalMetricDimensions: List[AdditionalDimension],
    credentialsProvider: Option[AWSCredentialsProvider],
    metricsCredentialsProvider: Option[AWSCredentialsProvider],
    aggregationEnabled: Boolean,
    aggregationMaxCount: Long,
    aggregationMaxSize: Long,
    cloudwatchEndpoint: Option[String],
    cloudwatchPort: Long,
    collectionMaxCount: Long,
    collectionMaxSize: Long,
    connectTimeout: Long,
    credentialsRefreshDelay: Long,
    enableCoreDumps: Boolean,
    failIfThrottled: Boolean,
    kinesisEndpoint: Option[String],
    kinesisPort: Long,
    logLevel: String,
    maxConnections: Long,
    metricsGranularity: String,
    metricsLevel: String,
    metricsNamespace: String,
    metricsUploadDelay: Long,
    minConnections: Long,
    nativeExecutable: Option[String],
    rateLimit: Long,
    recordMaxBufferedTime: Long,
    recordTtl: Long,
    region: Option[Regions],
    requestTimeout: Long,
    tempDirectory: Option[String],
    verifyCertificate: Boolean,
    threadingModel: ThreadingModel,
    threadPoolSize: Int
) {

  def toAwsConfig: KinesisProducerConfiguration = {
    val initial = new KinesisProducerConfiguration()
      .setAggregationEnabled(aggregationEnabled)
      .setAggregationMaxCount(aggregationMaxCount)
      .setAggregationMaxSize(aggregationMaxSize)
      .setCloudwatchPort(cloudwatchPort)
      .setCollectionMaxCount(collectionMaxCount)
      .setCollectionMaxSize(collectionMaxSize)
      .setConnectTimeout(connectTimeout)
      .setCredentialsRefreshDelay(credentialsRefreshDelay)
      .setEnableCoreDumps(enableCoreDumps)
      .setFailIfThrottled(failIfThrottled)
      .setKinesisPort(kinesisPort)
      .setLogLevel(logLevel)
      .setMaxConnections(maxConnections)
      .setMetricsGranularity(metricsGranularity)
      .setMetricsLevel(metricsLevel)
      .setMetricsNamespace(metricsNamespace)
      .setMetricsUploadDelay(metricsUploadDelay)
      .setMinConnections(minConnections)
      .setRateLimit(rateLimit)
      .setRecordMaxBufferedTime(recordMaxBufferedTime)
      .setRecordTtl(recordTtl)
      .setRequestTimeout(requestTimeout)
      .setVerifyCertificate(verifyCertificate)
      .setThreadingModel(threadingModel)
      .setThreadPoolSize(threadPoolSize)

    KinesisProducerConfig.setAdditionalDimensions(initial, additionalMetricDimensions)

    // This is ugly
    val wCredProv = credentialsProvider.fold(initial)(initial.setCredentialsProvider)
    val wMetricCredProv =
      metricsCredentialsProvider.fold(wCredProv)(wCredProv.setMetricsCredentialsProvider)
    val wCWEP       = cloudwatchEndpoint.fold(wMetricCredProv)(wMetricCredProv.setCloudwatchEndpoint)
    val wKinesisEP  = kinesisEndpoint.fold(wCWEP)(wCWEP.setKinesisEndpoint)
    val wNativeExec = nativeExecutable.fold(wKinesisEP)(wKinesisEP.setNativeExecutable)
    val wRegion     = region.fold(wNativeExec)(reg => wNativeExec.setRegion(reg.getName))
    val wTempDir    = tempDirectory.fold(wRegion)(wRegion.setTempDirectory)

    wTempDir
  }
}

object KinesisProducerConfig {
  def apply() = default

  def default: KinesisProducerConfig = KinesisProducerConfig(
    additionalMetricDimensions = List(),
    credentialsProvider = None,
    metricsCredentialsProvider = None,
    aggregationEnabled = true,
    aggregationMaxCount = 4294967295L,
    aggregationMaxSize = 51200,
    cloudwatchEndpoint = None,
    cloudwatchPort = 443,
    collectionMaxCount = 500,
    collectionMaxSize = 5242880,
    connectTimeout = 6000,
    credentialsRefreshDelay = 5000,
    enableCoreDumps = false,
    failIfThrottled = false,
    kinesisEndpoint = None,
    kinesisPort = 443,
    logLevel = "info",
    maxConnections = 24,
    metricsGranularity = "shard",
    metricsLevel = "detailed",
    metricsNamespace = "KinesisProducerLibrary",
    metricsUploadDelay = 60000,
    minConnections = 1,
    nativeExecutable = None,
    rateLimit = 150,
    recordMaxBufferedTime = 100,
    recordTtl = 30000,
    region = None,
    requestTimeout = 6000,
    tempDirectory = None,
    verifyCertificate = true,
    threadingModel = ThreadingModel.PER_REQUEST,
    threadPoolSize = 0
  )

  private def setAdditionalDimensions(
      conf: KinesisProducerConfiguration,
      dimensions: List[AdditionalDimension]
  ) = dimensions.foldLeft(conf) { (conf, dimension) =>
    conf.addAdditionalMetricsDimension(dimension.getKey,
                                       dimension.getValue,
                                       dimension.getGranularity)
    conf
  }
}
