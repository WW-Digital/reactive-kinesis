package com.weightwatchers.reactive.kinesis.common

import java.io.File
import java.nio.ByteBuffer

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import com.amazonaws.services.kinesis.leases.impl.{KinesisClientLease, KinesisClientLeaseSerializer, LeaseManager}
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
         |         batchTimeoutSeconds = 2
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

  val TestStreamName: String = self.suiteName
  val TestStreamNrOfMessagesPerShard: Long = 100
  val TestStreamNumberOfShards: Long       = 2

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

  protected def createLeaseTable(applicationName: String): Unit = {
    val manager = new LeaseManager[KinesisClientLease](s"$applicationName-$TestStreamName", dynamoClient, new KinesisClientLeaseSerializer())
    manager.createLeaseTableIfNotExists(1l, 1l)
    while(!manager.leaseTableExists()) Thread.sleep(100)
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
    val kcl = ConsumerConf(kinesisConfig(streamName = TestStreamName, appName = suiteName),
                           "testConsumer").kclConfiguration

    AmazonKinesisAsyncClientBuilder
      .standard()
      .withClientConfiguration(kcl.getKinesisClientConfiguration.withMaxConnections(2))
      .withEndpointConfiguration(
        new EndpointConfiguration(kcl.getKinesisEndpoint, kcl.getRegionName)
      )
      .withCredentials(TestCredentials.Credentials)
      .build()
  }

  lazy val dynamoClient: AmazonDynamoDB = {
    val kcl = ConsumerConf(kinesisConfig(streamName = TestStreamName, appName = suiteName),
                           "testConsumer").kclConfiguration

    AmazonDynamoDBClientBuilder
      .standard()
      .withClientConfiguration(kcl.getDynamoDBClientConfiguration.withMaxConnections(2))
      .withEndpointConfiguration(
        new EndpointConfiguration(kcl.getDynamoDBEndpoint, kcl.getRegionName)
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
