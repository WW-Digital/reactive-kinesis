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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

object ProducerEvent {

  /**
    * Creates a [[ProducerEvent]] by converting the provided String to an INDIRECT [[ByteBuffer]].
    * For large String payloads you may be able to improve performance by using a DIRECT [[ByteBuffer]].
    */
  def apply(partitionKey: String, payload: String): ProducerEvent = {
    val bytes = payload.getBytes(StandardCharsets.UTF_8)
    new ProducerEvent(partitionKey, ByteBuffer.wrap(bytes))
  }
}

/**
  * Defines an event to be published to Kinesis.
  */
case class ProducerEvent(partitionKey: String, payload: ByteBuffer)
