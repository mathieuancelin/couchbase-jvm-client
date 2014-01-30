package com.couchbase.client.scala.bucket

import scala.concurrent.{ExecutionContext, Future}
import com.couchbase.client.scala.view.{ViewQueryOptions, View, ViewResult}
import com.couchbase.client.scala.query.{QueryResult, Criteria}
import scala.util.Try
import java.io.{ObjectOutputStream, ByteArrayOutputStream, ObjectStreamClass}

trait Format[T] {
  def reads(bytes: Array[Byte]): Try[T]
  def writes(value: T): Array[Byte]
}

object DefaultFormat extends Format[String] {
  def reads(bytes: Array[Byte]): Try[String] = Try(new String(bytes))
  def writes(value: String): Array[Byte] = value.getBytes
}

class SerialFormat(cl: ClassLoader) extends Format[AnyRef] {

  def reads(bytes: Array[Byte]): Try[AnyRef] = {
    Try {
      new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(data)) {
        override protected def resolveClass(desc: ObjectStreamClass) = {
          Class.forName(desc.getName, false, cl)
        }
      }.readObject()
    }
  }

  def writes(value: AnyRef): Array[Byte] = {
    val bos: ByteArrayOutputStream = new ByteArrayOutputStream()
    new ObjectOutputStream(bos).writeObject(obj)
    bos.toByteArray
  }
}

/**
 * Operations that can be performed against a bucket.
 */
trait Bucket {

  def insert[T](document: Document[T])(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[Document[T]]
  def insert[T](id: String, value: T, expiration: Int = -1, groupId: Option[String] = None )(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[Document[T]]
  def insert[T](documents: TraversableOnce[Document[T]])(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[TraversableOnce[Document[T]]]

  def upsert[T](document: Document[T])(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[Document[T]]
  def upsert[T](id: String, value: T, expiration: Int = -1, groupId: Option[String] = None )(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[Document[T]]
  def upsert[T](documents: TraversableOnce[Document[T]])(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[TraversableOnce[Document[T]]]

  def replace[T](document: Document[T])(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[Document[T]]
  def replace[T](id: String, value: T, cas: Option[Long] = None, expiration: Int = -1, groupId: Option[String] = None )(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[Document[T]]
  def replace[T](documents: TraversableOnce[Document[T]])(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[TraversableOnce[Document[T]]]

  def update[T](document: Document[T])(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[Document[T]]
  def update[T](id: String, value: T, cas: Option[Long] = None, expiration: Int = -1, groupId: Option[String] = None )(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[Document[T]]
  def update[T](documents: TraversableOnce[Document[T]])(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[TraversableOnce[Document[T]]]

  def remove[T](document: Document[T])(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[Document[T]]
  def remove[T](id: String, cas: Option[Long] = None, groupId: Option[String] = None )(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[Document[T]]
  def remove[T](documents: TraversableOnce[Document[T]])(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[TraversableOnce[Document[T]]]

  def get[T](id: String, lock: Option[Int] = None, groupId: Option[String] = None )(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[Document[T]]
  def get[T](document: Document[T], lock: Option[Int] = None)(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[Document[T]]
  def get[T](documents: TraversableOnce[Document[T]], lock: Option[Int] = None)(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[TraversableOnce[Document[T]]]

  def endure[T](persistTo: PersistTo, replicateTo: ReplicateTo)(callback: Bucket => Unit)(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[Unit]

  def find[T](view: View)(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[TraversableOnce[Document[T]]]
  def find[T](criteria: Criteria)(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[TraversableOnce[Document[T]]]

  def unlock[T](document: Document[T])(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[Document[T]]
  def unlock[T](id: String, cas: Long, groupId: Option[String] = None )(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[Document[T]]

  def counter[T](document: Document[T], delta: Int, initial: Int = 0)(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[Document[T]]
  def counter[T](id: String, delta: Int, initial: Int = 0, expiration: Int = -1, groupId: Option[String] = None )(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[Document[T]]

  def view[T](designDocument: String, viewName: String, options: Option[ViewQueryOptions] = None)(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[ViewResult]
  def query[T](query: String)(implicit ec: ExecutionContext, format: Format[T] = DefaultFormat): Future[QueryResult]

  def flush: Future[Boolean]
  def info: Future[BucketInfo]

  def insertDesignDocument(designDocument: DesignDocument, overwrite: Boolean = true)(implicit ec: ExecutionContext): Future[DesignDocument]
  def insertDesignDocument(name: String, content: String, overwrite: Boolean = true)(implicit ec: ExecutionContext): Future[DesignDocument]
  def updateDesignDocument(designDocument: DesignDocument)(implicit ec: ExecutionContext): Future[DesignDocument]
  def updateDesignDocument(name: String, content: String)(implicit ec: ExecutionContext): Future[DesignDocument]
  def removeDesignDocument(name: String)(implicit ec: ExecutionContext): Future[DesignDocument]
  def removeDesignDocument(designDocument: DesignDocument)(implicit ec: ExecutionContext): Future[DesignDocument]
  def getDesignDocument(name: String)(implicit ec: ExecutionContext): Future[DesignDocument]
  def listDesignDocuments: Future[TraversableOnce[DesignDocument]]
}

