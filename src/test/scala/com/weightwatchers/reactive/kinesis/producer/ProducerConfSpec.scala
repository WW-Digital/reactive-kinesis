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

import java.io.File

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FreeSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

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

  val kinesisConfig2 = ConfigFactory
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
      val producerConf = ProducerConf(kinesisConfig2, "testProducer")

      producerConf.streamName should be("core-test-kinesis-producer")
      producerConf.dispatcher should be(Some("kinesis.akka.custom-dispatcher"))
      producerConf.throttlingConf.get.maxOutstandingRequests should be(50000)
      producerConf.throttlingConf.get.retryDuration should be(100.millis)

      val kplConfig = kinesisConfig2.getConfig("testProducer.kpl")

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

        println(s"${field.getName}=${field.get(kplLibConfiguration)}")

        withClue(
          s"Property `$configKey` was not as expected when asserting the KPL configuration: "
        ) {
          kplConfig.hasPath(configKey) should be(true)
          field.get(kplLibConfiguration).toString should be(kplConfig.getString(configKey))
        }
      }

    }

  }

}

//scalastyle:on
