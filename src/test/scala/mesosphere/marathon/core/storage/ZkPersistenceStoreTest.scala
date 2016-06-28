package mesosphere.marathon.core.storage

import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID

import akka.Done
import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import mesosphere.AkkaUnitTest
import mesosphere.marathon.StoreCommandFailedException
import mesosphere.marathon.core.storage.impl.zk.{ InMemoryStore, RamStoreFormat, ZkPersistenceStore }
import mesosphere.marathon.integration.setup.ProcessKeeper
import mesosphere.marathon.test.zk.NoRetryPolicy
import mesosphere.util.PortAllocator
import mesosphere.util.state.zk.RichCuratorFramework
import org.apache.commons.io.FileUtils
import org.apache.curator.framework.{ CuratorFramework, CuratorFrameworkFactory }
import org.scalatest.BeforeAndAfterAll

import scala.util.{ Failure, Success }

private[storage] case class TestClass1(str: String, int: Int)
private[storage] object TestClass1 {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  implicit val byteStringMarshaller: Marshaller[TestClass1, ByteString] =
    Marshaller.opaque { (tc: TestClass1) =>
      val bytes = ByteString.newBuilder
      val strBytes = tc.str.getBytes(StandardCharsets.UTF_8)
      bytes.putInt(strBytes.length)
      bytes.putBytes(strBytes)
      bytes.putInt(tc.int)
      bytes.result()
    }

  implicit val byteStringUnmarhsaller: Unmarshaller[ByteString, TestClass1] =
    Unmarshaller.strict { (bytes: ByteString) =>
      val it = bytes.iterator
      val strLen = it.getInt
      val str = new String(it.getBytes(strLen), StandardCharsets.UTF_8)
      TestClass1(str, it.getInt)
    }

  implicit val zkIdResolver = new IdResolver[String, TestClass1, ByteString] {
    override def fromStorageId(path: String): String = {
      require(path.startsWith("/test-class/"))
      path.replaceFirst("/test-class/", "")
    }

    override def toStorageId(id: String): String = if (id.nonEmpty) s"/test-class/$id" else "/test-class"
  }

  implicit val ramStoreMarshaller = Marshaller.opaque { tc: TestClass1 => RamStoreFormat(tc) }
  implicit val ramStoreUnmarshaller = Unmarshaller.strict { raw: RamStoreFormat =>
    raw.value.asInstanceOf[TestClass1]
  }
  implicit val ramStoreResolver = new IdResolver[String, TestClass1, RamStoreFormat] {
    override def toStorageId(id: String): String = if (id.nonEmpty) s"/testclass/$id" else "/testclass"
    override def fromStorageId(id: String): String = if (id.nonEmpty) id.replaceFirst("/testclass/", "") else id
  }

}

private[storage] trait PersistenceStoreTest { this: AkkaUnitTest =>
  val rootId: String
  def createId: String
  def singleTypeStore[Serialized](store: => PersistenceStore[String, Serialized])(implicit
    ir: IdResolver[String, TestClass1, Serialized],
    m: Marshaller[TestClass1, Serialized],
    um: Unmarshaller[Serialized, TestClass1]): Unit = {

    "list nothing at the root" in {
      store.ids(rootId).runWith(Sink.seq).futureValue should equal(Nil)
    }
    "list nothing at a random folder" in {
      store.ids(createId).runWith(Sink.seq).futureValue should equal(Nil)
    }
    "create and then read an object" in {
      val tc = TestClass1("abc", 1)
      store.create("task-1", tc).futureValue should be(Done)
      store.get("task-1").futureValue.value should equal(tc)
    }
    "create then list an object" in {
      val tc = TestClass1("abc", 2)
      store.create("task-2", tc).futureValue should be(Done)
      store.ids(rootId).runWith(Sink.seq).futureValue should contain("task-2")
    }
    "not allow an object to be created if it already exists" in {
      val tc = TestClass1("abc", 3)
      store.create("task-3", tc).futureValue should be(Done)
      store.create("task-3", tc).failed.futureValue shouldBe a[StoreCommandFailedException]
    }
    "create an object at a nested path" in {
      val tc = TestClass1("abc", 3)
      store.create("nested/object", tc).futureValue should be(Done)
      store.get("nested/object").futureValue.value should equal(tc)
      store.ids(rootId).runWith(Sink.seq).futureValue should contain("nested")
      store.ids("nested").runWith(Sink.seq).futureValue should contain theSameElementsAs Seq("object")
    }
    "create two objects at a nested path" in {
      val tc1 = TestClass1("a", 1)
      val tc2 = TestClass1("b", 2)
      store.create("nested-2/1", tc1).futureValue should be(Done)
      store.create("nested-2/2", tc2).futureValue should be(Done)
      store.ids(rootId).runWith(Sink.seq).futureValue should contain("nested-2")
      store.ids("nested-2").runWith(Sink.seq).futureValue should contain theSameElementsAs Seq("1", "2")
      store.get("nested-2/1").futureValue.value should be(tc1)
      store.get("nested-2/2").futureValue.value should be(tc2)
    }
    "delete idempotently" in {
      store.create("delete-me", TestClass1("def", 3)).futureValue should be(Done)
      store.delete("delete-me").futureValue should be(Done)
      store.delete("delete-me").futureValue should be(Done)
      store.ids(rootId).runWith(Sink.seq).futureValue should not contain ("delete-me")
    }
    "update an object" in {
      val created = TestClass1("abc", 2)
      val updated = TestClass1("def", 3)
      store.create("update/1", created).futureValue should be(Done)
      var calledWithTc = Option.empty[TestClass1]
      store.update("update/1") { old: TestClass1 =>
        calledWithTc = Option(old)
        Success(updated)
      }.futureValue should equal(created)
      calledWithTc.value should equal(created)
      store.get("update/1").futureValue.value should equal(updated)
    }
    "not update an object that doesn't exist" in {
      store.update("update/2") { old: TestClass1 =>
        Success(TestClass1("abc", 3))
      }.failed.futureValue shouldBe a[StoreCommandFailedException]
    }
    "not update an object if the callback returns a failure" in {
      val tc = TestClass1("abc", 3)
      store.create("update/3", tc).futureValue should be(Done)
      store.update("update/3") { _: TestClass1 =>
        Failure[TestClass1](new NotImplementedError)
      }.futureValue should equal(tc)
      store.get("update/3").futureValue.value should equal(tc)
    }
  }
}

/*
class ZkPersistenceStoreTest extends AkkaUnitTest with PersistenceStoreTest with BeforeAndAfterAll {
  val zkPort = PortAllocator.ephemeralPort()
  val zkDir = Files.createTempDirectory(suiteName)

  def rootClient: CuratorFramework = {
    val client = CuratorFrameworkFactory.newClient(s"127.0.0.1:$zkPort", NoRetryPolicy)
    client.start()
    client
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    zkDir.toFile.deleteOnExit()
    ProcessKeeper.startZooKeeper(zkPort, zkDir.toFile.getAbsolutePath)
  }

  override def afterAll: Unit = {
    ProcessKeeper.stopAllProcesses()
    FileUtils.deleteDirectory(zkDir.toFile)
    super.afterAll()
  }

  val rootId: String = ""

  def createId: String = s"${UUID.randomUUID().toString.replaceAll("-", "_")}"

  def randomClient: RichCuratorFramework = {
    val path = UUID.randomUUID().toString.replaceAll("-", "_")
    rootClient.blockUntilConnected()
    rootClient.create().creatingParentContainersIfNeeded().forPath(s"/$path")
    new RichCuratorFramework(rootClient.usingNamespace(path))
  }

  lazy val emptyStore: ZkPersistenceStore = new ZkPersistenceStore(randomClient)

  "ZookeeperPersistenceStore" should {
    behave like singleTypeStore(emptyStore)
  }
}

class RamStorePersistenceTest extends AkkaUnitTest with PersistenceStoreTest {
  val rootId: String = ""

  def createId: String = s"${UUID.randomUUID().toString.replaceAll("-", "_")}"

  lazy val emptyStore: InMemoryStore = new InMemoryStore()

  "InMemoryStore" should {
    behave like singleTypeStore(emptyStore)
  }
}
*/
class DifferentStores extends AkkaUnitTest with PersistenceStoreTest with BeforeAndAfterAll {
  val zkPort = PortAllocator.ephemeralPort()
  val zkDir = Files.createTempDirectory(suiteName)

  def rootClient: CuratorFramework = {
    val client = CuratorFrameworkFactory.newClient(s"127.0.0.1:$zkPort", NoRetryPolicy)
    client.start()
    client
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    zkDir.toFile.deleteOnExit()
    ProcessKeeper.startZooKeeper(zkPort, zkDir.toFile.getAbsolutePath)
  }

  override def afterAll: Unit = {
    ProcessKeeper.stopAllProcesses()
    FileUtils.deleteDirectory(zkDir.toFile)
    super.afterAll()
  }

  val rootId: String = ""

  def createId: String = s"${UUID.randomUUID().toString.replaceAll("-", "_")}"

  def randomClient: RichCuratorFramework = {
    val path = UUID.randomUUID().toString.replaceAll("-", "_")
    rootClient.blockUntilConnected()
    rootClient.create().creatingParentContainersIfNeeded().forPath(s"/$path")
    new RichCuratorFramework(rootClient.usingNamespace(path))
  }

  lazy val zkStore: ZkPersistenceStore = new ZkPersistenceStore(randomClient)
  lazy val ramStore: InMemoryStore = new InMemoryStore()

  "PersistenceStore" when {
    "backed by ZK" should {
      behave like singleTypeStore(zkStore)
    }
    "backed by InMemory" should {
      behave like singleTypeStore(ramStore)
    }
  }
}