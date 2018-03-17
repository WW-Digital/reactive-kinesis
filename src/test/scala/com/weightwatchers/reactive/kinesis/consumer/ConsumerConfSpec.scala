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

import java.io.File

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer.ConsumerConf
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration.DurationInt

//scalastyle:off magic.number
class ConsumerConfSpec
    extends TestKit(ActorSystem("consumer-conf-spec"))
    with ImplicitSender
    with FreeSpecLike
    with Matchers
    with MockitoSugar
    with BeforeAndAfterAll
    with GivenWhenThen
    with ScalaFutures {

  val defaultKinesisConfig =
    ConfigFactory.parseFile(new File("src/main/resources/reference.conf")).getConfig("kinesis")

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(3, Seconds), interval = Span(50, Millis))

  val kinesisConfig = ConfigFactory
    .parseString(
      """
        |kinesis {
        |
        |   application-name = "TestSpec"
        |
        |   test-consumer-config {
        |      stream-name = "some-other-stream"
        |
        |      worker {
        |         batchTimeoutSeconds = 1234
        |         gracefulShutdownHook = false
        |         shutdownTimeoutSeconds = 2
        |      }
        |
        |      checkpointer {
        |         backoffMillis = 4321
        |      }
        |
        |      kcl {
        |         AWSCredentialsProvider = DefaultAWSCredentialsProviderChain
        |
        |         regionName = us-east-2
        |
        |         # Default: LATEST
        |         initialPositionInStream = TRIM_HORIZON
        |
        |         # Default = 10000
        |         maxRecords = 20000
        |
        |         # Default = 1000
        |         idleTimeBetweenReadsInMillis = 1234
        |
        |         # Default: 10000
        |         failoverTimeMillis = 11000
        |
        |         # Default: 60000
        |         shardSyncIntervalMillis = 70000
        |
        |         # Default: true
        |         cleanupLeasesUponShardCompletion = false
        |
        |         # Default: true
        |         validateSequenceNumberBeforeCheckpointing = false
        |
        |         # Default: null
        |         kinesisEndpoint = "https://kinesis"
        |
        |         # Default: null
        |         dynamoDBEndpoint = "https://dynamo"
        |
        |         # Default: false
        |         callProcessRecordsEvenForEmptyRecordList = true
        |
        |         # Default: 10000
        |         parentShardPollIntervalMillis = 40000
        |
        |         # Default: 500
        |         taskBackoffTimeMillis = 600
        |
        |         # Default: 10000
        |         metricsBufferTimeMillis = 10001
        |
        |
        |         # Default: 10000
        |         metricsMaxQueueSize = 10009
        |
        |
        |         # Default: DETAILED
        |         metricsLevel = NONE
        |
        |
        |         # Default: Operation, ShardId
        |         metricsEnabledDimensions = Operation
        |
        |
        |         # Default: 2147483647 (Integer.MAX_VALUE)
        |         maxLeasesForWorker = 11111111
        |
        |
        |         # Default: 1
        |         maxLeasesToStealAtOneTime = 2
        |
        |
        |         # Default: 10
        |         initialLeaseTableReadCapacity = 15
        |
        |
        |         # Default: 10
        |         initialLeaseTableWriteCapacity = 14
        |
        |         # Default: false
        |         skipShardSyncAtStartupIfLeasesExist=true
        |
        |
        |         # Default: <applicationName>
        |         userAgent = testy123
        |
        |         # Default = <applicationName>
        |         tableName = meh
        |
        |         # Default: 20
        |         maxLeaseRenewalThreads=9
        |
        |
        |         # Default: no timeout
        |         timeoutInSeconds = 10
        |         
        |
        |         # The amount of milliseconds to wait before graceful shutdown forcefully terminates.
        |         # Default: 5000
        |         shutdownGraceMillis = 2500
        |
        |         # If retryGetRecordsInSeconds is set
        |         # And maxGetRecordsThreadPool is set
        |         # Then use getRecords will asynchronously retry internally using a CompletionService of
        |         # max size maxGetRecordsThreadPool with "retryGetRecordsInSeconds" between each retry
        |         #
        |         # Time in seconds to wait before the worker retries to get a record
        |         # Default: Optional value, default not set
        |         retryGetRecordsInSeconds = 2
        |
        |         # max number of threads in the getRecords thread pool
        |         # Default: Optional value, default not set
        |         maxGetRecordsThreadPool = 1
        |         
        |         #
        |         # Pre-fetching config
        |         #
        |         
        |         # Pre-fetching will retrieve and queue additional records from Kinesis while the
        |         # application is processing existing records.
        |         # Pre-fetching can be enabled by setting dataFetchingStrategy to PREFETCH_CACHED. Once
        |         # enabled an additional fetching thread will be started to retrieve records from Kinesis. 
        |         # Retrieved records will be held in a queue until the application is ready to process them.
        |         
        |         # Which data fetching strategy to use (DEFAULT, PREFETCH_CACHED)
        |         # Default: DEFAULT
        |         dataFetchingStrategy = DEFAULT
        |         
        |         #
        |         # Pre-fetching supports the following configuration values:
        |         #
        |         
        |         # The maximum number of process records input that can be queued
        |         # Default: 3
        |         maxPendingProcessRecordsInput = 3
        |         
        |         # The maximum number of bytes that can be queued
        |         # Default 8388608 (8 * 1024 * 1024 / 8Mb)
        |         maxCacheByteSize = 8388608
        |         
        |         # The maximum number of records that can be queued
        |         # Default: 30000
        |         maxRecordsCount = 30000
        |         
        |         # The amount of time to wait between calls to Kinesis
        |         # Default: 1500
        |         idleMillisBetweenCalls = 1500
        |         
        |         #
        |         # End of Pre-fetching config
        |         #
        |
        |         # Milliseconds after which the logger will log a warning message for the long running task
        |         # Default: not set
        |         logWarningForTaskAfterMillis = 100
        |
        |         # True if we should ignore child shards which have open parents
        |         # Default: not set
        |         ignoreUnexpectedChildShards = false
        |      }
        |
        |   }
        |}
      """.stripMargin
    )
    .getConfig("kinesis")
    .withFallback(defaultKinesisConfig)

  "The Consumerconf" - {

    "Should parse test-consumer-config into a ConsumerConf, setting all properties in the KinesisClientLibConfiguration" in {
      // This will fail when fields are added or renamed in the KCL
      //
      // The properties are automatically tested reflectively against the underlying implementation
      // First we load each property reflectively
      // Then we parse and try to load it from our config
      // If it isn't present we fail the test

      // We reflectively load fields, but setters are used in the implementation.
      // However some setters don't match the field names (?!?), this renames them on the fly.
      val confToFieldConversions = Map(
        "skipShardSyncAtStartupIfLeasesExist" -> "skipShardSyncAtWorkerInitializationIfLeasesExist",
        "maxCacheByteSize"                    -> "maxByteSize"
      )

      // RecordsFetcherFactory is a nested object in the configurator, so we define it's fields manually as a hack
      val recordFetcherFields = List(
        "maxPendingProcessRecordsInput",
        "maxCacheByteSize",
        "maxRecordsCount",
        "idleMillisBetweenCalls",
        "dataFetchingStrategy"
      )

      // Some fields dion't have setters / aren't configurable
      val fieldsToSkip = List(
        "useragent", //this gets nested internally
        "streamname",
        "timestampatinitialpositioninstream",
        "commonclientconfig",
        "shardprioritizationstrategy",
        "kinesisclientconfig",
        "dynamodbclientconfig",
        "cloudwatchclientconfig",
        "credentialsprovider", //these must be tested individually
        "applicationname"
      )

      val consumerConf = ConsumerConf(kinesisConfig, "test-consumer-config")

      consumerConf.workerConf.batchTimeout should be(1234.seconds)
      consumerConf.workerConf.failedMessageRetries should be(1)
      consumerConf.workerConf.failureTolerancePercentage should be(0.25)
      consumerConf.workerConf.shutdownHook should be(false)
      consumerConf.workerConf.shutdownTimeout should be(Timeout(2.seconds))
      consumerConf.checkpointerConf.backoff should be(4321.millis)
      consumerConf.checkpointerConf.interval should be(2000.millis)          //reference default
      consumerConf.checkpointerConf.notificationDelay should be(1000.millis) //reference default
      consumerConf.dispatcher should be(Some("kinesis.akka.default-dispatcher"))
      consumerConf.kclConfiguration.getApplicationName should be(
        "TestSpec-some-other-stream"
      )

      val kclConfig             = kinesisConfig.getConfig("test-consumer-config.kcl")
      val kclLibConfiguration   = consumerConf.kclConfiguration
      val recordsFetcherFactory = kclLibConfiguration.getRecordsFetcherFactory

      //We're dealing with Java classes so using Java reflection is cleaner here
      //Start with the setters to prevent picking up all the unrelated private fields, stripping the "with"
      val configKeys = kclLibConfiguration.getClass.getDeclaredMethods
        .filter(_.getName.startsWith("with"))
        .map(_.getName.drop(4))
        .map(field => field.head.toLower + field.tail)
        .filterNot(
          field => fieldsToSkip.contains(field.toLowerCase)
        )

      configKeys foreach { configKey =>
        //Hack to deal with the nested objects
        //The "with" setters live on the kclLibConfiguration, but they defer and set the value on the nested object
        //We need to know which object to assert, so we use a pre-defined list
        //TODO we could do this automatically if we used getFields to load all fields and traversed down, combining the lists
        val obj =
          if (recordFetcherFields.contains(configKey)) recordsFetcherFactory
          else kclLibConfiguration

        val field =
          obj.getClass.getDeclaredField(
            confToFieldConversions.getOrElse(configKey, configKey)
          )
        field.setAccessible(true)

        withClue(
          s"Property `$configKey` was missing or incorrect when asserting the KCL configuration - possibly a newly added KCL property: "
          //The property should be defined in the test config above, and also in the reference.conf with full description and defaults - commented out.
        ) {
          kclConfig.hasPath(configKey) should be(true)
          field.get(obj).toString should include(kclConfig.getString(configKey))
        }
      }

    }
  }
}

//scalastyle:on
