/*
 Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.util.Properties

import com.typesafe.config.Config

import scala.collection.JavaConverters._

/**
  * Extensions added to Typesafe config class.
  *
  * @see https://github.com/cakesolutions/scala-kafka-client/blob/master/client/src/main/scala/cakesolutions/kafka/TypesafeConfigExtensions.scala
  */
object TypesafeConfigExtensions {

  implicit class RichConfig(val config: Config) extends AnyVal {

    /**
      * Convert Typesafe config to Java `Properties`.
      */
    def toProperties: Properties = {
      val props = new Properties()
      config
        .entrySet()
        .asScala
        .foreach(entry => props.put(entry.getKey, entry.getValue.unwrapped().toString))
      props
    }
  }

}
