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

package com.weightwatchers.reactive.kinesis.models

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import org.joda.time.DateTime
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FreeSpec, Matchers}

class ConsumerEventSpec extends FreeSpec with Matchers with GeneratorDrivenPropertyChecks {

  "A ConsumerEvent" - {
    "can be read as string" in {
      forAll(Gen.alphaNumStr) { string =>
        val event = ConsumerEvent(CompoundSequenceNumber("123", 0),
                                  ByteBuffer.wrap(string.getBytes(UTF_8)),
                                  DateTime.now())
        event.payloadAsString(UTF_8) shouldBe string
      }
    }
  }
}
