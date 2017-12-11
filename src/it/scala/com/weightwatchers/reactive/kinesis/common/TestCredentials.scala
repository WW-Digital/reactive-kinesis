package com.weightwatchers.reactive.kinesis.common

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider}

class TestCredentials(accessKey: String, secretKey: String)
    extends AWSCredentials
    with AWSCredentialsProvider {

  override def getAWSAccessKeyId: String = accessKey

  override def getAWSSecretKey: String = secretKey

  override def getCredentials: AWSCredentials = this

  override def refresh: Unit = {}
}

object TestCredentials {
  val Credentials = new TestCredentials("foo", "bar")
}
