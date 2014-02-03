package com.couchbase.client.scala.bucket

trait PersistTo {
  def name: String
}

case object MEM extends PersistTo   { val name = "MEM" }
case object ONE extends PersistTo   { val name = "ONE" }
case object TWO extends PersistTo   { val name = "TWO" }
case object THREE extends PersistTo { val name = "THREE" }
case object FOUR extends PersistTo  { val name = "FOUR" }
case object FIVE extends PersistTo  { val name = "FIVE" }
