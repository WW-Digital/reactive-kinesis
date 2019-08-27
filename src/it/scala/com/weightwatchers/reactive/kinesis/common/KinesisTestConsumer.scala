package com.weightwatchers.reactive.kinesis.common

import java.util.Collections

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord
import com.amazonaws.services.kinesis.model._
import com.amazonaws.services.kinesis.{AmazonKinesisAsyncClient, _}
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer.ConsumerConf

import scala.jdk.CollectionConverters._
import scala.concurrent.duration.FiniteDuration

object KinesisTestConsumer {

  /**
    * Creates an instance of the test consumer from the configuration.
    * If the endpoint has been specified this will be used instead of deriving it from the region.
    */
  def from(config: ConsumerConf, requestTimeout: Option[FiniteDuration]): KinesisTestConsumer =
    from(
      config.kclConfiguration.getKinesisCredentialsProvider,
      Option(config.kclConfiguration.getKinesisEndpoint),
      Option(config.kclConfiguration.getRegionName),
      requestTimeout,
      config.kclConfiguration.getKinesisClientConfiguration
    )

  /**
    * Creates an instance of the test consumer from the configuration.
    * If the endpoint has been specified this will be used instead of deriving it from the region.
    */
  def from(credentialsProvider: AWSCredentialsProvider,
           endpoint: Option[String],
           region: Option[String],
           requestTimeout: Option[FiniteDuration],
           config: ClientConfiguration = new ClientConfiguration()): KinesisTestConsumer = {

    requestTimeout.foreach(timeout => config.setRequestTimeout(timeout.toMillis.toInt))

    val builder = AmazonKinesisAsyncClient
      .asyncBuilder()
      .withClientConfiguration(config)
      .withCredentials(credentialsProvider)

    endpoint.fold(builder.withRegion(region.getOrElse("us-east-1"))) { endpoint =>
      builder.withEndpointConfiguration(
        new EndpointConfiguration(endpoint, region.getOrElse("us-east-1"))
      )
    }

    new KinesisTestConsumer(builder.build())
  }
}

/**
  * A test consumer which retrieves a batch of messages from Kinesis for validating a producer.
  *
  * Does not handle checkpointing.
  *
  * The point at which you create this consumer determines the point at which the messages are received from.
  *
  * To elaborate, the shardIterator is created as "LATEST", so only messages received after creation of this will be received.
  *
  */
class KinesisTestConsumer(client: AmazonKinesis) {

  /**
    * Retrieves a batch of records from Kinesis as Strings.
    *
    * Handles de-aggregation.
    */
  def retrieveRecords(streamName: String, batchSize: Int): List[String] = {
    getShards(streamName)
      .flatMap { shard =>
        val getRecordsRequest = new GetRecordsRequest
        getRecordsRequest.setShardIterator(getShardIterator(streamName, shard))
        getRecordsRequest.setLimit(batchSize)
        client.getRecords(getRecordsRequest).getRecords.asScala.toList
      }
      .flatMap { record: Record =>
        UserRecord
          .deaggregate(Collections.singletonList(record))
          .asScala
          .map { ur =>
            new String(ur.getData.array(), java.nio.charset.StandardCharsets.UTF_8)
          }
      }
  }

  private def getShardIterator(streamName: String, shard: Shard) = {
    client
      .getShardIterator(streamName, shard.getShardId, "TRIM_HORIZON")
      .getShardIterator
  }

  private def getShards(streamName: String) = {
    client
      .describeStream(streamName)
      .getStreamDescription
      .getShards
      .asScala
      .toList
  }

  def shutdown(): Unit = client.shutdown()

}
