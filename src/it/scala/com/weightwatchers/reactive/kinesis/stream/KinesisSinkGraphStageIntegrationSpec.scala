package com.weightwatchers.reactive.kinesis.stream

import akka.stream.scaladsl.Source
import com.weightwatchers.reactive.kinesis.common.{AkkaUnitTestLike, IntegrationTest, KinesisConfiguration, KinesisSuite}
import com.weightwatchers.reactive.kinesis.models.ProducerEvent
import scala.concurrent.duration._

class KinesisSinkGraphStageIntegrationSpec
    extends IntegrationTest
    with KinesisSuite
    with KinesisConfiguration
    with AkkaUnitTestLike
{

  "KinesisSinkGraph" - {

    "produced messages are written to the stream" in new withKinesisConfForApp("sink_produce") {
      val messageCount = 100
      val elements     = 1.to(messageCount).map(_.toString)
      Source(elements)
        .map(num => ProducerEvent(num, num))
        .runWith(Kinesis.sink(producerConf()))
        .futureValue
      val list = testConsumer.retrieveRecords(TestStreamName, messageCount)
      list should contain allElementsOf elements
      testConsumer.shutdown()
    }

    "upstream fail should fail the materialized value of the sink" in new withKinesisConfForApp(
      "sink_fail"
    ) {
      Source
        .failed(new IllegalStateException("Boom"))
        .runWith(Kinesis.sink(producerConf()))
        .failed
        .futureValue shouldBe a[IllegalStateException]
    }
  }

  // do not create messages in setup, we will create messages inside the test
  override def TestStreamNrOfMessagesPerShard: Long    = 0
  override implicit def patienceConfig: PatienceConfig = PatienceConfig(60.seconds, 1.second)
}
