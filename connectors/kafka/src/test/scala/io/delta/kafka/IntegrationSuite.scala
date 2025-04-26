package io.delta.kafka

import io.delta.table.DeltaCatalog
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory
import org.testcontainers.containers.ComposeContainer
import org.testcontainers.containers.wait.strategy.Wait

import java.io.File
import java.time.Duration

class IntegrationSuite extends AnyFunSuite with BeforeAndAfterAll {

  private val LOG = LoggerFactory.getLogger(classOf[IntegrationSuite])

  val CONNECT_PORT = 8083

  var container: ComposeContainer = _

  def startContainer(): Unit = {
    container = new ComposeContainer(new File("./docker/docker-compose.yml"))
      .withStartupTimeout(Duration.ofMinutes(2))
      .waitingFor("connect", Wait.forHttp("/connectors"))
    container.start()

    LOG.info(s"Start Container")
  }

  val jsonBody = """
    {
      "tasks.max": "1",
      "topics": "payments",
      "connector.class": "io.delta.kafka.DeltaSinkConnector",
      "iceberg.catalog.s3.endpoint": "http://minio:9000",
      "iceberg.catalog.s3.secret-access-key": "minioadmin",
      "iceberg.catalog.s3.access-key-id": "minioadmin",
      "iceberg.catalog.s3.path-style-access": "true",
      "iceberg.catalog.catalog-impl": "io.delta.table.DeltaCatalog",
      "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
      "iceberg.catalog.warehouse": "s3a://warehouse/",
      "iceberg.tables.auto-create-enabled": "true",
      "iceberg.catalog.client.region": "eu-west-1",
      "iceberg.control.commitIntervalMs": "1000",
      "iceberg.tables": "payments",
      "value.converter.schemas.enable": "false",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "schemas.enable": "false"
    }
  """

  def startConnector(): Unit = {
    val response = requests.put(
      url = "http://localhost:8083/connectors/DeltaSinkConnector/config",
      headers = Map(
        "Content-Type" -> "application/json",
        "Accept" -> "application/json"
      ),
      data = jsonBody
    )

    LOG.info(s"Start Connector Status code: ${response.statusCode}")
    LOG.info(s"Start Connector Response body: ${response.text()}")
  }

  def stopContainer(): Unit = {
    container.stop()
  }

  override def beforeAll(): Unit = {
    startContainer()
    startConnector()
  }

  override def afterAll(): Unit = {
    stopContainer()
  }

  test("normal test") {
    Thread.sleep(10 * 60 * 1000)
  }
}
