package com.weightwatchers.reactive.kinesis.common

import java.io.File
import java.nio.ByteBuffer

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import com.amazonaws.services.kinesis.leases.impl.{
  KinesisClientLease,
  KinesisClientLeaseSerializer,
  LeaseManager
}
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.services.kinesis.{AmazonKinesisAsync, AmazonKinesisAsyncClientBuilder}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer.ConsumerConf
import com.weightwatchers.reactive.kinesis.producer.ProducerConf
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Suite}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

/**
  * Base trait to create a KinesisConfiguration from application config + override options.
  */
trait KinesisConfiguration {

  val defaultKinesisConfig: Config =
    ConfigFactory.parseFile(new File("src/main/resources/reference.conf")).getConfig("kinesis")

  private def kinesisConfig(streamName: String,
                            appName: String = "integration-test",
                            workerId: String = "",
                            maxRecords: Int = 10000): Config =
    ConfigFactory
      .parseString(
        s"""
           |kinesis {
           |
           |   application-name = "$appName"
           |
           |   testProducer {
           |      stream-name = "$streamName"
           |
           |      akka.max-outstanding-requests = 10
           |
           |      kpl {
           |         Region = us-east-1
           |
           |         CloudwatchEndpoint = localhost
           |         CloudwatchPort = 4582
           |
           |         KinesisEndpoint = localhost
           |         KinesisPort = 4568
           |
           |         VerifyCertificate = false
           |      }
           |   }
           |
           |   testConsumer {
           |      # The name of the consumer stream, MUST be specified per consumer
           |      stream-name = "$streamName"
           |
           |      # Use localstack for integration test
           |      kcl {
           |         kinesisEndpoint = "https://localhost:4568"
           |         dynamoDBEndpoint = "https://localhost:4569"
           |
           |         AWSCredentialsProvider = "com.weightwatchers.reactive.kinesis.common.TestCredentials|foo|bar"
           |
           |         regionName = us-east-1
           |
           |         workerId = "$workerId"
           |
           |         # Reduce default values, to speed up the integration test.
           |         maxRecords = $maxRecords
           |         metricsLevel = NONE
           |         failoverTimeMillis = 1000
           |         shardSyncIntervalMillis = 1000
           |         idleTimeBetweenReadsInMillis = 200
           |         parentShardPollIntervalMillis = 1000
           |      }
           |
           |      worker {
           |         batchTimeoutSeconds = 4
           |         failedMessageRetries = 0
           |         failureTolerancePercentage = 0
           |         gracefulShutdownHook = false
           |         shutdownTimeoutSeconds = 10
           |      }
           |   }
           |}
      """.stripMargin
      )
      .getConfig("kinesis")
      .withFallback(defaultKinesisConfig)

  def consumerConfFromConfig(conf: Config, consumer: String = "testConsumer"): ConsumerConf = {
    val config     = ConsumerConf(conf, consumer)
    val clientConf = config.kclConfiguration.getKinesisClientConfiguration
    // reduce the thread pool to a small size (default is 50)
    // this config option is missing via typesafe config and should be added.
    config.copy(
      kclConfiguration =
        config.kclConfiguration.withKinesisClientConfig(clientConf.withMaxConnections(5))
    )
  }

  def consumerConfFor(streamName: String,
                      appName: String = "integration-test",
                      workerId: String = "",
                      maxRecords: Int = 10000): ConsumerConf = {

    val config =
      ConsumerConf(kinesisConfig(streamName, appName, workerId, maxRecords), "testConsumer")
    val clientConf = config.kclConfiguration.getKinesisClientConfiguration

    // reduce the thread pool to a small size (default is 50)
    // this config option is missing via typesafe config and should be added.
    // TODO - not exposed in KinesisClientLibConfiguration (and therefore configurator), needs hack or KCL PR
    config.copy(
      kclConfiguration =
        config.kclConfiguration.withKinesisClientConfig(clientConf.withMaxConnections(5))
    )
  }

  def producerConfFor(streamName: String, appName: String = "integration-test"): ProducerConf = {
    ProducerConf(kinesisConfig(streamName, appName),
                 "testProducer",
                 Some(TestCredentials.Credentials))
  }
}

/**
  * Mixin this trait to your test to setup Kinesis for integration tests.
  *
  * Every suite will have a clean Kinesis and Dynamo as well as one Stream
  * with `TestStreamNumberOfShards` Shards with `TestStreamNrOfMessagesPerShard` Messages each.
  *
  * The ApplicationName used for each test should be different, this will prevent tests from interfering with each other.
  * i.e. Different applications will checkpoint and consume the data separately.
  *
  * Deletes only the table and stream for the CURRENT test (TestStreamName) once the Suite has completed.
  *
  */
trait KinesisSuite
    extends BeforeAndAfter
    with BeforeAndAfterAll
    with StrictLogging
    with KinesisConfiguration { self: Suite =>

  def TestStreamName: String = self.suiteName

  def TestStreamNrOfMessagesPerShard: Long

  def TestStreamNumberOfShards: Long = 1

  private lazy val kclSetupConfig =
    consumerConfFor(streamName = TestStreamName, appName = suiteName).kclConfiguration

  /**
    * Cleanup dynamo before each test
    */
  before {
    cleanDynamo()
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    // clean up from eventually last run
    cleanKinesis()

    // create new stream
    createKinesisStream()

    // Pumping test data inside.
    createTestData(TestStreamNrOfMessagesPerShard.toInt)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    dynamoClient.shutdown()
    kinesisClient.shutdown()
  }

  /**
    * Wrap your spec in this to make Kinesis configuration easily available for the application name.
    * Also ensures the lease table is initialised correctly.
    *
    * @param appName the name of the application
    */
  class withKinesisConfForApp(val appName: String) {

    /**
      * A simple consumer, useful for validating the behaviour of producers.
      */
    lazy val testConsumer: KinesisTestConsumer =
      KinesisTestConsumer.from(consumerConfFor(appName = appName, streamName = TestStreamName),
                               Some(100.millis))

    def consumerConf(batchSize: Long = TestStreamNrOfMessagesPerShard): ConsumerConf = {
      consumerConfFor(streamName = TestStreamName,
                      appName = appName,
                      maxRecords = math.max(1, batchSize.toInt))
    }

    def producerConf(): ProducerConf = producerConfFor(TestStreamName, appName)

    // proactively create the lease table for this application.
    // KCL does not handle this reliably, which makes the test brittle.
    createLeaseTable(appName)
  }

  protected def createKinesisStream(): Unit = {
    kinesisClient.createStream(TestStreamName, TestStreamNumberOfShards.toInt)

    // Block until the stream is ready to rumble.
    while (kinesisClient
             .describeStream(TestStreamName)
             .getStreamDescription
             .getStreamStatus != "ACTIVE") {
      Thread.sleep(100)
    }
    logger.info(s"Stream: $TestStreamName is created.")
  }

  /**
    * There seems to be a race condition in KCL when the lease table is created/accessed, which can make the tests brittle.
    * This method creates the lease table proactively to remedy this shortcoming.
    */
  private def createLeaseTable(applicationName: String): Unit = {
    val manager = new LeaseManager[KinesisClientLease](s"$applicationName-$TestStreamName",
                                                       dynamoClient,
                                                       new KinesisClientLeaseSerializer())
    manager.createLeaseTableIfNotExists(1l, 1l)
    while (!manager.leaseTableExists()) Thread.sleep(100)
  }

  protected def cleanKinesis(): Unit = {
    // We delete our stream if it exist.
    kinesisClient.listStreams().getStreamNames.asScala.toList.find(_ == TestStreamName).foreach {
      kinesisClient.deleteStream
    }

    // Blocking until it is really deleted.
    while (kinesisClient.listStreams().getStreamNames.contains(TestStreamName)) {
      Thread.sleep(100)
    }
  }

  protected def cleanDynamo(): Unit = {
    val result = dynamoClient.listTables()
    result.getTableNames.asScala.foreach { tableName =>
      logger.info(s"Delete dynamo table $tableName")
      dynamoClient.deleteTable(tableName)
    }
  }

  lazy val kinesisClient: AmazonKinesisAsync = {
    AmazonKinesisAsyncClientBuilder
      .standard()
      .withClientConfiguration(kclSetupConfig.getKinesisClientConfiguration)
      .withEndpointConfiguration(
        new EndpointConfiguration(kclSetupConfig.getKinesisEndpoint, kclSetupConfig.getRegionName)
      )
      .withCredentials(TestCredentials.Credentials)
      .build()
  }

  lazy val dynamoClient: AmazonDynamoDB = {
    AmazonDynamoDBClientBuilder
      .standard()
      .withClientConfiguration(kclSetupConfig.getDynamoDBClientConfiguration.withMaxConnections(2))
      .withEndpointConfiguration(
        new EndpointConfiguration(kclSetupConfig.getDynamoDBEndpoint, kclSetupConfig.getRegionName)
      )
      .withCredentials(TestCredentials.Credentials)
      .build()
  }

  protected def createTestData(testDataCount: Int): Unit = {
    import scala.collection.JavaConverters._

    kinesisClient
      .describeStream(TestStreamName)
      .getStreamDescription
      .getShards
      .asScala
      .toList
      .map(_.getShardId)
      .foreach { shardId =>
        (1 to testDataCount).foreach { nr =>
          val msg = new PutRecordRequest()
            .withData(ByteBuffer.wrap(nr.toString.getBytes))
            .withStreamName(TestStreamName)
            .withPartitionKey(shardId)
          kinesisClient.putRecord(msg)
        }
      }
  }
}
