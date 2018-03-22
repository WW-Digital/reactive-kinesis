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

package com.weightwatchers.reactive.kinesis.models

import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}

import org.joda.time.DateTime

/**
  * A combination of sequence and subsequence numbers.
  * See: http://docs.aws.amazon.com/streams/latest/dev/kinesis-kpl-consumer-deaggregation.html
  */
case class CompoundSequenceNumber(sequenceNumber: String, subSequenceNumber: Long) {
  def >=(that: CompoundSequenceNumber): Boolean = //scalastyle:ignore
    (this.sequenceNumber + this.subSequenceNumber) >= (that.sequenceNumber + that.subSequenceNumber)

  def >(that: CompoundSequenceNumber): Boolean = //scalastyle:ignore
    (this.sequenceNumber + this.subSequenceNumber) > (that.sequenceNumber + that.subSequenceNumber)

  def <(that: CompoundSequenceNumber): Boolean = //scalastyle:ignore
    (this.sequenceNumber + this.subSequenceNumber) < (that.sequenceNumber + that.subSequenceNumber)

  def <=(that: CompoundSequenceNumber): Boolean = //scalastyle:ignore
    (this.sequenceNumber + this.subSequenceNumber) <= (that.sequenceNumber + that.subSequenceNumber)
}

object CompoundSequenceNumber {
  implicit def orderingBySeqAndSubSeq[A <: CompoundSequenceNumber]: Ordering[A] =
    Ordering.by(e => (e.sequenceNumber, e.subSequenceNumber))
}

/**
  * The actual event we're processing (contained within [[com.weightwatchers.reactive.kinesis.consumer.ConsumerWorker.ProcessEvent]]
  */
case class ConsumerEvent(sequenceNumber: CompoundSequenceNumber,
                         payload: ByteBuffer,
                         timestamp: DateTime) {

  /**
    * Convenience method to read the payload as string.
    * Note: make sure the sender has created a string payload.
    * @param charset the character set to interpret the byte array.
    * @return the payload as string.
    */
  def payloadAsString(charset: Charset = StandardCharsets.UTF_8) =
    new String(payload.array(), charset)
}
