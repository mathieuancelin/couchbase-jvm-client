package com.couchbase.client.scala.bucket

case class Document[T](id: String, content: Option[T], cas: Long, expiration: Int, groupId: String)
