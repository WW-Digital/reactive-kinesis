package com.weightwatchers.reactive.kinesis.common

import java.io.File
import java.nio.ByteBuffer

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.services.kinesis.{AmazonKinesisAsync, AmazonKinesisAsyncClientBuilder}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer.ConsumerConf
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Suite}

import scala.collection.JavaConverters._

/**
  * Base trait to create a KinesisConfiguration from application config + override options.
  */
trait KinesisConfiguration {

  val defaultKinesisConfig =
    ConfigFactory.parseFile(new File("src/main/resources/reference.conf")).getConfig("kinesis")

  def kinesisConfig(streamName: String,
                    appName: String = "integration-test",
                    workerId: String = "",
                    maxRecords: Int = 10000) =
    ConfigFactory
      .parseString(
        s"""
         |kinesis {
         |
         |   application-name = "$appName"
         |
         |   # The name of the consumer, we can have many consumers per application
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
         |         # dramatically reduce default values.
         |         # This will speed up the integration test by factor 20x or greater
         |         maxRecords = $maxRecords
         |         metricsLevel = NONE
         |         failoverTimeMillis = 500
         |         shardSyncIntervalMillis = 1000
         |         idleTimeBetweenReadsInMillis = 100
         |         parentShardPollIntervalMillis = 1000
         |      }
         |
         |      worker {
         |         batchTimeoutSeconds = 1
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
}

/**
  * Mixin this trait to your test to interact with Kinesis.
  * Every suite will have a clean Kinesis and Dynamo as well as one Stream with 2 Shards with 100 Messages each.
  *
  * Deletes only the table and stream for the CURRENT test (TestStreamName).
  *
  */
trait KinesisKit
    extends BeforeAndAfter
    with BeforeAndAfterAll
    with StrictLogging
    with KinesisConfiguration { self: Suite =>

  val TestStreamName: String

  val TestStreamNrOfMessagesPerShard: Long = 100
  val TestStreamNumberOfShards: Long       = 2

  /**
    * Cleanup dynamo before each test
    */
  before {
    cleanDynamo()
  }

  override protected def beforeAll(): Unit = {
    val kinesis = kinesisClient()

    // clean up from eventually last run
    cleanKinesis(kinesis)

    // create new stream
    createKinesisStream(kinesis)

    // Pumping test data inside.
    createTestData(TestStreamNrOfMessagesPerShard.toInt, kinesis)

    kinesis.shutdown()
  }

  protected def createKinesisStream(kinesisClient: AmazonKinesisAsync): Unit = {

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

  protected def cleanKinesis(kinesisClient: AmazonKinesisAsync): Unit = {
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
    val dynamo = dynamoClient()
    cleanDynamo(dynamo)
    dynamo.shutdown()
  }

  protected def cleanDynamo(dynamoClient: AmazonDynamoDB): Unit = {
    val result = dynamoClient.listTables()
    result.getTableNames.asScala.foreach { tableName =>
      logger.info(s"Delete dynamo table $tableName")
      dynamoClient.deleteTable(tableName)
    }
  }

  protected def kinesisClient(): AmazonKinesisAsync = {
    val kcl = ConsumerConf(kinesisConfig(streamName = TestStreamName, appName = suiteName),
                           "testConsumer").kclConfiguration

    AmazonKinesisAsyncClientBuilder
      .standard()
      .withClientConfiguration(kcl.getKinesisClientConfiguration)
      .withEndpointConfiguration(
        new EndpointConfiguration(kcl.getKinesisEndpoint, kcl.getRegionName)
      )
      .withCredentials(TestCredentials.Credentials)
      .build()
  }

  protected def dynamoClient(): AmazonDynamoDB = {
    val kcl = ConsumerConf(kinesisConfig(streamName = TestStreamName, appName = suiteName),
                           "testConsumer").kclConfiguration

    AmazonDynamoDBClientBuilder
      .standard()
      .withClientConfiguration(kcl.getDynamoDBClientConfiguration)
      .withEndpointConfiguration(
        new EndpointConfiguration(kcl.getDynamoDBEndpoint, kcl.getRegionName)
      )
      .withCredentials(TestCredentials.Credentials)
      .build()
  }

  protected def createTestData(testDataCount: Int, client: AmazonKinesisAsync): Unit = {
    import scala.collection.JavaConverters._

    client
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
          client.putRecord(msg)
        }
      }
  }
}
