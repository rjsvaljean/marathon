package mesosphere.marathon.core.storage.impl.zk

import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import akka.{Done, NotUsed}
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.StoreCommandFailedException
import mesosphere.marathon.core.storage.{IdResolver, PersistenceStore}
import mesosphere.marathon.util.toRichFuture
import mesosphere.util.state.zk.{Children, GetData, RichCuratorFramework}
import org.apache.zookeeper.KeeperException.NoNodeException

import scala.async.Async.{async, await}
import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

class ZkPersistenceStore(client: RichCuratorFramework)(implicit ctx: ExecutionContext, mat: ActorMaterializer)
    extends PersistenceStore[String, ByteString] with StrictLogging {

  override def ids[V](parent: String)(implicit pr: IdResolver[String, V, ByteString]): Source[String, NotUsed] = {
    val childrenFuture = async {
      val children = await(client.children(pr.toStorageId(parent)).asTry)
      children match {
        case Success(Children(_, _, nodes)) =>
          nodes
        case Failure(_: NoNodeException) =>
          Seq.empty[String]
        case Failure(e) =>
          throw new StoreCommandFailedException(s"Unable to get children of: $parent", e)
      }
    }
    Source.fromFuture(childrenFuture).mapConcat(identity)
  }

  override protected def set(id: String, v: ByteString): Future[Done] = client.setData(id, v).map(_ => Done).recover {
    case NonFatal(e) => throw new StoreCommandFailedException(s"Unable to update: $id", e)
  }

  override protected def createRaw(id: String, v: ByteString): Future[Done] = {
    client.create(id,
      Some(v),
      creatingParentContainersIfNeeded = true,
      creatingParentsIfNeeded = true).map(_ => Done).recover {
        case NonFatal(e) => throw new StoreCommandFailedException(s"Unable to create: $id", e)
    }
  }

  override def get[V](id: String)(implicit
    pr: IdResolver[String, V, ByteString],
    um: Unmarshaller[ByteString, V]): Future[Option[V]] = async {

    val path = pr.toStorageId(id)
    val data = await(client.data(path).asTry)
    data match {
      case Success(GetData(_, _, bytes)) =>
        Some(await(Unmarshal(bytes).to[V]))
      case Failure(_: NoNodeException) =>
        None
      case Failure(e) =>
        throw new StoreCommandFailedException(s"Unable to get $id", e)
    }
  }

  override def delete[V](id: String)(implicit pr: IdResolver[String, V, ByteString]): Future[Done] = async {
    val path = pr.toStorageId(id)
    await(client.delete(path).asTry) match {
      case Success(_) | Failure(_: NoNodeException) => Done
      case Failure(e) =>
        throw new StoreCommandFailedException(s"Unable to delete $id", e)
    }
  }
}
