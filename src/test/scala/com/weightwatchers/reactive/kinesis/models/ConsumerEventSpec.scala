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

package com.weightwatchers.reactive.kinesis.models

import com.weightwatchers.reactive.kinesis.UnitTest
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import org.joda.time.DateTime
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class ConsumerEventSpec extends UnitTest with ScalaCheckDrivenPropertyChecks {

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
