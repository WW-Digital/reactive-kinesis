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
                         payload: String,
                         timestamp: DateTime)
