package mesosphere.marathon.core.storage.impl.zk

import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiFunction

import akka.http.scaladsl.unmarshalling.{ Unmarshal, Unmarshaller }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.{ Done, NotUsed }
import mesosphere.marathon.core.storage.{ IdResolver, PersistenceStore }

import scala.annotation.tailrec
import scala.collection.immutable.Seq
import scala.concurrent.{ ExecutionContext, Future }

case class RamStoreFormat(value: Any)

/**
  * TODO: Actually make the tree of children work.
  * @param ctx
  * @param mat
  */
class InMemoryStore(implicit ctx: ExecutionContext, mat: ActorMaterializer)
    extends PersistenceStore[String, RamStoreFormat] {
  private val store = new ConcurrentHashMap[String, RamStoreFormat]()
  private val children = new ConcurrentHashMap[String, Set[String]]()

  override def ids[V](parent: String)(implicit pr: IdResolver[String, V, RamStoreFormat]): Source[String, NotUsed] = {
    val childIds = Option(children.get(pr.toStorageId(parent))).getOrElse(Seq.empty[String])
    Source(childIds.map(pr.fromStorageId))
  }

  override def get[V](id: String)(implicit
    pr: IdResolver[String, V, RamStoreFormat],
    um: Unmarshaller[RamStoreFormat, V]): Future[Option[V]] = {
    Option(store.get(pr.toStorageId(id))).fold(Future.successful(Option.empty[V])) { raw =>
      Unmarshal(raw).to[V].map(Some(_))
    }
  }

  override def delete[V](k: String)(implicit pm: IdResolver[String, V, RamStoreFormat]): Future[Done] = {
    store.remove(pm.toStorageId(k))
    var path = pm.toStorageId(k).split("/").toVector
    while (path.nonEmpty) {
      children.remove(path.mkString("/"))
      path = path.dropRight(1)
    }
    Future.successful(Done)
  }

  override protected def set(id: String, v: RamStoreFormat): Future[Done] = {
    store.put(id, v)
    Future.successful(Done)
  }

  override protected def createRaw(id: String, v: RamStoreFormat): Future[Done] = {
    val path = id.split("/").toList
    @tailrec def addChildren(path: String, remaining: Seq[String]): Unit = remaining match {
      case head :: tail =>
        children.compute(path, new BiFunction[String, Set[String], Set[String]] {
          override def apply(t: String, u: Set[String]): Set[String] = Option(u).fold(Set(head))(_ + head)
        })
        addChildren(s"$path/$head", tail)
      case _ =>
    }
    if (path.nonEmpty)
      addChildren(s"${path.head}", path.tail)

    store.put(id, v)
    Future.successful(Done)
  }
}
