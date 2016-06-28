package mesosphere.marathon.core.storage

import akka.http.scaladsl.marshalling.{ Marshal, Marshaller }
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.scaladsl.Source
import akka.{ Done, NotUsed }
import mesosphere.marathon.StoreCommandFailedException
import mesosphere.util.CallerThreadExecutionContext

import scala.async.Async._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

trait IdResolver[K, +V, +Serialized] {
  def toStorageId(id: K): String
  def fromStorageId(id: String): K
}

trait PersistenceStore[K, Serialized] {
  def ids[V](parent: K)(implicit pr: IdResolver[K, V, Serialized]): Source[K, NotUsed]

  def get[V](id: K)(implicit pr: IdResolver[K, V, Serialized], um: Unmarshaller[Serialized, V]): Future[Option[V]]

  def create[V](id: K, v: V)(implicit
    pr: IdResolver[K, V, Serialized],
    m: Marshaller[V, Serialized],
    um: Unmarshaller[Serialized, V]): Future[Done] = {
    createOrUpdate(id, (oldValue: Option[V]) => oldValue match {
      case None => Success(v)
      case Some(existing) =>
        Failure(new StoreCommandFailedException(s"Unable to create $id as it already exists ($existing)"))
    }).map(_ => Done)(CallerThreadExecutionContext.callerThreadExecutionContext)
  }

  def update[V](id: K)(change: V => Try[V])(implicit
    pr: IdResolver[K, V, Serialized],
    um: Unmarshaller[Serialized, V],
    m: Marshaller[V, Serialized]): Future[V] = {
    createOrUpdate(id, (oldValue: Option[V]) => oldValue match {
      case Some(old) =>
        change(old)
      case None =>
        Failure(new StoreCommandFailedException(s"Unable to update $id as it doesn't exist"))
    }).map(_.get)(CallerThreadExecutionContext.callerThreadExecutionContext)
  }

  def delete[V](k: K)(implicit pm: IdResolver[K, V, Serialized]): Future[Done]

  private def createOrUpdate[V](
    id: K,
    change: Option[V] => Try[V])(implicit
    pr: IdResolver[K, V, Serialized],
    m: Marshaller[V, Serialized],
    um: Unmarshaller[Serialized, V],
    ctx: ExecutionContext = ExecutionContext.global): Future[Option[V]] = async {
    val path = pr.toStorageId(id)
    val old = await(get(id))
    change(old) match {
      case Success(newValue) =>
        val serialized = await(Marshal(newValue).to[Serialized])
        old match {
          case Some(_) =>
            await(set(path, serialized))
            old
          case None =>
            await(createRaw(path, serialized))
            old
        }
      case Failure(error: StoreCommandFailedException) =>
        throw error
      case Failure(error) =>
        old
    }
  }

  protected def set(id: String, v: Serialized): Future[Done]
  protected def createRaw(id: String, v: Serialized): Future[Done]
}
