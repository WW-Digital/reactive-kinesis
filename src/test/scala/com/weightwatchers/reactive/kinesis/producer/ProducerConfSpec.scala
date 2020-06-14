/*
 * Copyright 2017 WeightWatchers
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

import java.io.File

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.amazonaws.auth.ContainerCredentialsProvider.ECSCredentialsEndpointProvider
import com.amazonaws.auth.{
  DefaultAWSCredentialsProviderChain,
  EC2ContainerCredentialsProviderWrapper,
  EnvironmentVariableCredentialsProvider
}
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration.ThreadingModel
import com.amazonaws.services.kinesis.producer.protobuf.Config.AdditionalDimension
import com.typesafe.config.ConfigFactory
import org.apache.http.client.CredentialsProvider
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FreeSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

//scalastyle:off magic.number
class ProducerConfSpec
    extends TestKit(ActorSystem("producer-spec"))
    with ImplicitSender
    with FreeSpecLike
    with Matchers
    with MockitoSugar
    with BeforeAndAfterAll {

  val defaultKinesisConfig =
    ConfigFactory.parseFile(new File("src/main/resources/reference.conf")).getConfig("kinesis")

  val kinesisConfig = ConfigFactory
    .parseString(
      """
        |kinesis {
        |
        |   application-name = "TestSpec"
        |
        |   testProducer {
        |      stream-name = "core-test-kinesis-producer"
        |
        |      akka {
        |         dispatcher = "kinesis.akka.custom-dispatcher"
        |
        |         max-outstanding-requests = 50000
        |
        |         throttling-retry-millis = 100
        |      }
        |
        |      kpl {
        |         # Default: true
        |         AggregationEnabled = false
        |
        |         # Default: 4294967295
        |         # Minimum: 1
        |         # Maximum (inclusive): 9223372036854775807
        |         AggregationMaxCount = 5
        |
        |         # Default: 51200
        |         # Minimum: 64
        |         # Maximum (inclusive): 1048576
        |         AggregationMaxSize = 77
        |
        |         # Default: 500
        |         # Minimum: 1
        |         # Maximum (inclusive): 500
        |         CollectionMaxCount = 25
        |
        |         # Default: 5242880
        |         # Minimum: 52224
        |         # Maximum (inclusive): 9223372036854775807
        |         CollectionMaxSize = 55555
        |
        |         # Default: 6000
        |         # Minimum: 100
        |         # Maximum (inclusive): 300000
        |         ConnectTimeout = 101
        |
        |
        |         # Default: 5000
        |         # Minimum: 1
        |         # Maximum (inclusive): 300000
        |         CredentialsRefreshDelay = 2400
        |
        |         # Expected pattern: ^([A-Za-z0-9-\\.]+)?$
        |         CloudwatchEndpoint = 127.0.0.1
        |
        |         # Default: 443
        |         # Minimum: 1
        |         # Maximum (inclusive): 65535
        |         CloudwatchPort = 123
        |
        |         # Default: false
        |         EnableCoreDumps = true
        |
        |         # Use a custom Kinesis endpoint.
        |         #
        |         # Mostly for testing use. Note this does not accept protocols or paths, only
        |         # host names or ip addresses. There is no way to disable TLS. The KPL always
        |         # connects with TLS.
        |         #
        |         # Expected pattern: ^([A-Za-z0-9-\\.]+)?$
        |         KinesisEndpoint = 172.1.1.1
        |
        |         # Default: 443
        |         # Minimum: 1
        |         # Maximum (inclusive): 65535
        |         KinesisPort = 666
        |
        |         # Default: false
        |         FailIfThrottled = true
        |
        |         # Default: info
        |         # Expected pattern: info|warning|error
        |         LogLevel = warning
        |
        |         # Default: 24
        |         # Minimum: 1
        |         # Maximum (inclusive): 256
        |         MaxConnections = 5
        |
        |         # Default: shard
        |         # Expected pattern: global|stream|shard
        |         MetricsGranularity = stream
        |
        |         # Default: detailed
        |         # Expected pattern: none|summary|detailed
        |         MetricsLevel = none
        |
        |         # Default: KinesisProducerLibrary
        |         # Expected pattern: (?!AWS/).{1,255}
        |         MetricsNamespace = SomeNamespace
        |
        |         # Default: 60000
        |         # Minimum: 1
        |         # Maximum (inclusive): 60000
        |         MetricsUploadDelay = 5000
        |
        |         # Default: 1
        |         # Minimum: 1
        |         # Maximum (inclusive): 16
        |         MinConnections = 3
        |
        |         #Path to the native KPL binary. Only use this setting if you want to use a custom build of
        |         #the native code.
        |         NativeExecutable=/tmp
        |
        |         # Default: 150
        |         # Minimum: 1
        |         # Maximum (inclusive): 9223372036854775807
        |         RateLimit = 99
        |
        |         # Default: 100
        |         # Maximum (inclusive): 9223372036854775807
        |         RecordMaxBufferedTime = 88
        |
        |         # Default: 30000
        |         # Minimum: 100
        |         # Maximum (inclusive): 9223372036854775807
        |         RecordTtl = 25000
        |
        |         # Expected pattern: ^([a-z]+-[a-z]+-[0-9])?$
        |         Region = "us-east-2"
        |
        |         # Default: 6000
        |         # Minimum: 100
        |         # Maximum (inclusive): 600000
        |         RequestTimeout = 3000
        |
        |         # If not specified, defaults to /tmp in Unix. (Windows TBD)
        |         TempDirectory = /tmp
        |
        |         # Default: true
        |         VerifyCertificate = false
        |
        |         # Enum:
        |         # PER_REQUEST: Tells the native process to create a thread for each request.
        |         # POOLED: Tells the native process to use a thread pool. The size of the pool can be controlled by ThreadPoolSize
        |         # Default = PER_REQUEST
        |         ThreadingModel = POOLED
        |
        |         # Default: 0
        |         ThreadPoolSize = 5
        |      }
        |
        |   }
        |}
      """.stripMargin
    )
    .getConfig("kinesis")
    .withFallback(defaultKinesisConfig)

  implicit val timeout = Timeout(5.seconds)

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, timeout.duration)
  }

  "The ProducerConf" - {

    "Should parse the Config into a ProducerConf, setting all properties in the KinesisProducerConfiguration" in {
      // This will fail when fields are added or renamed in the KPL
      //
      // The properties are automatically tested reflectively against the underlying implementation
      // First we load each property reflectively
      // Then we parse and try to load it from our config
      // If it isn't present we fail the test

      val producerConf = ProducerConf(kinesisConfig, "testProducer")

      producerConf.streamName should be("core-test-kinesis-producer")
      producerConf.dispatcher should be(Some("kinesis.akka.custom-dispatcher"))
      producerConf.throttlingConf.get.maxOutstandingRequests should be(50000)
      producerConf.throttlingConf.get.retryDuration should be(100.millis)

      val kplConfig = kinesisConfig.getConfig("testProducer.kpl")

      val kplLibConfiguration = producerConf.kplLibConfiguration
      kplLibConfiguration.isAggregationEnabled should be(false)

      //We're dealing with Java classes so using Java reflection is cleaner here
      //Start with the setters to prevent picking up all the unrelated private fields, stripping the "set"
      val configKeys = kplLibConfiguration.getClass.getDeclaredMethods
        .filter(_.getName.startsWith("set"))
        .map(_.getName.drop(3))
        // TODO we don't yet support setting of credentials providers via config due to KPL limitations
        // TODO see issue #29 : https://github.com/WW-Digital/reactive-kinesis/issues/29
        .filterNot(_.toLowerCase.contains("credentialsprovider"))

      configKeys foreach { configKey =>
        val field =
          kplLibConfiguration.getClass.getDeclaredField(configKey.head.toLower + configKey.tail)
        field.setAccessible(true)

        withClue(
          s"Property `$configKey` was missing or incorrect when asserting the KPL configuration - possibly a newly added KPL property: "
        ) {
          kplConfig.hasPath(configKey) should be(true)
          field.get(kplLibConfiguration).toString should be(kplConfig.getString(configKey))
        }
      }

    }
  }

  "The KinesisProducerConfig" - {
    "Should convert properly to an AWS KinesisProducerConfiguration object" in {
      def randomLong                              = Random.nextLong().abs
      def randomLongInRange(start: Int, end: Int) = start + Random.nextInt((end - start) + 1).toLong
      def randomString(chars: Int)                = Random.alphanumeric.take(chars).mkString("")

      val testAdditionalDimensions = List(
        AdditionalDimension
          .newBuilder()
          .setKey("key")
          .setValue("value")
          .setGranularity(List("global", "stream", "shard")(Random.nextInt(3)))
          .build()
      )

      val testCredentialsProvider        = new EnvironmentVariableCredentialsProvider()
      val testMetricsCredentialsProvider = new EC2ContainerCredentialsProviderWrapper()
      val testAggregationEnabled         = Random.nextBoolean()
      val testAggregationMaxCount        = randomLong
      val testAggregationMaxSize         = randomLongInRange(64, 1048576)
      val testCloudwatchEndpoint         = s"www.cloudwatch.com"
      val testCloudwatchPort             = randomLongInRange(1, 65535)
      val testCollectionMaxCount         = randomLongInRange(1, 500)
      val testCollectionMaxSize          = randomLong
      val testConnectTimeout             = randomLongInRange(100, 300000)
      val testCredentialsRefreshDelay    = randomLongInRange(1, 300000)
      val testEnableCoreDumps            = Random.nextBoolean()
      val testFailIfThrottled            = Random.nextBoolean()
      val testKinesisEndpoint            = s"www.kinesis.com"
      val testKinesisPort                = randomLongInRange(1, 65535)
      val testLogLevel                   = List("info", "warning", "error")(Random.nextInt(3))
      val testMaxConnections             = randomLongInRange(1, 256)
      val testMetricsGranularity         = List("global", "stream", "shard")(Random.nextInt(3))
      val testMetricsLevel               = List("none", "summary", "detailed")(Random.nextInt(3))
      val testMetricsNamespace           = randomString(5)
      val testMetricsUploadDelay         = randomLongInRange(1, 60000)
      val testMinConnections             = randomLongInRange(1, 16)
      val testNativeExecutable           = s"executable.${randomString(3)}"
      val testRateLimit                  = randomLong
      val testRecordMaxBufferedTime      = randomLong
      val testRecordTtl                  = randomLong
      val testRegion = List(Regions.AP_NORTHEAST_1,
                            Regions.AP_SOUTH_1,
                            Regions.US_EAST_1,
                            Regions.US_WEST_1)(Random.nextInt(4))
      val testRequestTimeout    = randomLongInRange(100, 600000)
      val testTempDirectory     = s"/${randomString(10)}"
      val testVerifyCertificate = Random.nextBoolean()
      val testThreadingModel =
        List(ThreadingModel.PER_REQUEST, ThreadingModel.POOLED)(Random.nextInt(2))
      val testThreadPoolSize = Random.nextInt.abs

      val config = KinesisProducerConfig(
        additionalMetricDimensions = testAdditionalDimensions,
        credentialsProvider = Some(testCredentialsProvider),
        metricsCredentialsProvider = Some(testMetricsCredentialsProvider),
        aggregationEnabled = testAggregationEnabled,
        aggregationMaxCount = testAggregationMaxCount,
        aggregationMaxSize = testAggregationMaxSize,
        cloudwatchEndpoint = Some(testCloudwatchEndpoint),
        cloudwatchPort = testCloudwatchPort,
        collectionMaxCount = testCollectionMaxCount,
        collectionMaxSize = testCollectionMaxSize,
        connectTimeout = testConnectTimeout,
        credentialsRefreshDelay = testCredentialsRefreshDelay,
        enableCoreDumps = testEnableCoreDumps,
        failIfThrottled = testFailIfThrottled,
        kinesisEndpoint = Some(testKinesisEndpoint),
        kinesisPort = testKinesisPort,
        logLevel = testLogLevel,
        maxConnections = testMaxConnections,
        metricsGranularity = testMetricsGranularity,
        metricsLevel = testMetricsLevel,
        metricsNamespace = testMetricsNamespace,
        metricsUploadDelay = testMetricsUploadDelay,
        minConnections = testMinConnections,
        nativeExecutable = Some(testNativeExecutable),
        rateLimit = testRateLimit,
        recordMaxBufferedTime = testRecordMaxBufferedTime,
        recordTtl = testRecordTtl,
        region = Some(testRegion),
        requestTimeout = testRequestTimeout,
        tempDirectory = Some(testTempDirectory),
        verifyCertificate = testVerifyCertificate,
        threadingModel = testThreadingModel,
        threadPoolSize = testThreadPoolSize
      ).toAwsConfig

      config.getCredentialsProvider should be(testCredentialsProvider)
      config.getMetricsCredentialsProvider should be(testMetricsCredentialsProvider)
      config.getAggregationMaxCount should be(testAggregationMaxCount)
      config.getAggregationMaxSize should be(testAggregationMaxSize)
      config.getCloudwatchEndpoint should be(testCloudwatchEndpoint)
      config.getCloudwatchPort should be(testCloudwatchPort)
      config.getCollectionMaxCount should be(testCollectionMaxCount)
      config.getCollectionMaxSize should be(testCollectionMaxSize)
      config.getConnectTimeout should be(testConnectTimeout)
      config.getCredentialsRefreshDelay should be(testCredentialsRefreshDelay)
      config.isEnableCoreDumps should be(testEnableCoreDumps)
      config.isFailIfThrottled should be(testFailIfThrottled)
      config.getKinesisEndpoint should be(testKinesisEndpoint)
      config.getKinesisPort should be(testKinesisPort)
      config.getLogLevel should be(testLogLevel)
      config.getMaxConnections should be(testMaxConnections)
      config.getMetricsGranularity should be(testMetricsGranularity)
      config.getMetricsLevel should be(testMetricsLevel)
      config.getMetricsNamespace should be(testMetricsNamespace)
      config.getMetricsUploadDelay should be(testMetricsUploadDelay)
      config.getMinConnections should be(testMinConnections)
      config.getNativeExecutable should be(testNativeExecutable)
      config.getRateLimit should be(testRateLimit)
      config.getRecordMaxBufferedTime should be(testRecordMaxBufferedTime)
      config.getRecordTtl should be(testRecordTtl)
      config.getRegion should be(testRegion.getName)
      config.getRequestTimeout should be(testRequestTimeout)
      config.getTempDirectory should be(testTempDirectory)
      config.isVerifyCertificate should be(testVerifyCertificate)
      config.getThreadingModel should be(testThreadingModel)
      config.getThreadPoolSize should be(testThreadPoolSize)
    }
  }

  "Default method should match provided defaults in KinesisProducerConfiguration" in {
    val awsConfig        = KinesisProducerConfig().toAwsConfig
    val defaultAwsConfig = new KinesisProducerConfiguration()

    awsConfig.getCredentialsProvider.getClass.getName should be(
      defaultAwsConfig.getCredentialsProvider.getClass.getName
    )
    awsConfig.getMetricsCredentialsProvider should be(
      defaultAwsConfig.getMetricsCredentialsProvider
    )
    awsConfig.getAggregationMaxCount should be(defaultAwsConfig.getAggregationMaxCount)
    awsConfig.getAggregationMaxSize should be(defaultAwsConfig.getAggregationMaxSize)
    awsConfig.getCloudwatchEndpoint should be(defaultAwsConfig.getCloudwatchEndpoint)
    awsConfig.getCloudwatchPort should be(defaultAwsConfig.getCloudwatchPort)
    awsConfig.getCollectionMaxCount should be(defaultAwsConfig.getCollectionMaxCount)
    awsConfig.getCollectionMaxSize should be(defaultAwsConfig.getCollectionMaxSize)
    awsConfig.getConnectTimeout should be(defaultAwsConfig.getConnectTimeout)
    awsConfig.getCredentialsRefreshDelay should be(defaultAwsConfig.getCredentialsRefreshDelay)
    awsConfig.isEnableCoreDumps should be(defaultAwsConfig.isEnableCoreDumps)
    awsConfig.isFailIfThrottled should be(defaultAwsConfig.isFailIfThrottled)
    awsConfig.getKinesisEndpoint should be(defaultAwsConfig.getKinesisEndpoint)
    awsConfig.getKinesisPort should be(defaultAwsConfig.getKinesisPort)
    awsConfig.getLogLevel should be(defaultAwsConfig.getLogLevel)
    awsConfig.getMaxConnections should be(defaultAwsConfig.getMaxConnections)
    awsConfig.getMetricsGranularity should be(defaultAwsConfig.getMetricsGranularity)
    awsConfig.getMetricsLevel should be(defaultAwsConfig.getMetricsLevel)
    awsConfig.getMetricsNamespace should be(defaultAwsConfig.getMetricsNamespace)
    awsConfig.getMetricsUploadDelay should be(defaultAwsConfig.getMetricsUploadDelay)
    awsConfig.getMinConnections should be(defaultAwsConfig.getMinConnections)
    awsConfig.getNativeExecutable should be(defaultAwsConfig.getNativeExecutable)
    awsConfig.getRateLimit should be(defaultAwsConfig.getRateLimit)
    awsConfig.getRecordMaxBufferedTime should be(defaultAwsConfig.getRecordMaxBufferedTime)
    awsConfig.getRecordTtl should be(defaultAwsConfig.getRecordTtl)
    awsConfig.getRegion should be(defaultAwsConfig.getRegion)
    awsConfig.getRequestTimeout should be(defaultAwsConfig.getRequestTimeout)
    awsConfig.getTempDirectory should be(defaultAwsConfig.getTempDirectory)
    awsConfig.isVerifyCertificate should be(defaultAwsConfig.isVerifyCertificate)
    awsConfig.getThreadingModel should be(defaultAwsConfig.getThreadingModel)
    awsConfig.getThreadPoolSize should be(defaultAwsConfig.getThreadPoolSize)
  }
}
//scalastyle:on
