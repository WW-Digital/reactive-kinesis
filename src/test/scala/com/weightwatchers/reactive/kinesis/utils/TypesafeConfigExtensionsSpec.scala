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

package com.weightwatchers.reactive.kinesis.utils

import com.typesafe.config.ConfigFactory
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FreeSpec, Matchers}

/**
  * Tests the implicit future conversions.
  */
class TypesafeConfigExtensionsSpec
    extends FreeSpec
    with Matchers
    with MockitoSugar
    with BeforeAndAfterAll {

  val kplConfig = ConfigFactory.parseString("""
      |kpl {
      |   AggregationEnabled = true
      |   AggregationMaxCount = 4294967295
      |   AggregationMaxSize = 51200
      |   CollectionMaxCount = 500
      |}
      |
    """.stripMargin).getConfig("kpl")

  //scalastyle:off magic.number
  "The RichConfig" - {

    "Should convert typesafe config key values into Java Properties" in {

      import TypesafeConfigExtensions._

      val javaProperties = kplConfig.toProperties

      javaProperties.size() should equal(4)
      javaProperties.getProperty("AggregationEnabled") should equal("true")
      javaProperties.getProperty("AggregationMaxCount") should equal("4294967295")
      javaProperties.getProperty("AggregationMaxSize") should equal("51200")
      javaProperties.getProperty("CollectionMaxCount") should equal("500")

    }
  }
  //scalastyle:on
}
