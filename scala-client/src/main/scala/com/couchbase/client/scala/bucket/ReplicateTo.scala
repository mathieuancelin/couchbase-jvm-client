package com.couchbase.client.scala.bucket

trait ReplicateTo {
    def name: String
}

case object MEM extends ReplicateTo   { val name = "MEM" }
case object ONE extends ReplicateTo   { val name = "ONE" }
case object TWO extends ReplicateTo   { val name = "TWO" }
case object THREE extends ReplicateTo { val name = "THREE" }
case object FOUR extends ReplicateTo  { val name = "FOUR" }
case object FIVE extends ReplicateTo  { val name = "FIVE" }
