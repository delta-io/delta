package io.delta.kafka

import io.minio.{BucketExistsArgs, MakeBucketArgs, MinioClient}
import io.delta.table.DeltaCatalog
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.{CatalogProperties, Schema}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory
import org.testcontainers.containers.ComposeContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.apache.iceberg.types.Types
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

import scala.jdk.CollectionConverters._
import java.io.File
import java.time.Duration
import java.util.Properties

class IntegrationSuite extends AnyFunSuite with BeforeAndAfterAll {

  private val LOG = LoggerFactory.getLogger(classOf[IntegrationSuite])

  val CONNECT_PORT = 8083

  var container: ComposeContainer = _

  val kafkaProducer = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](props)
  }

  def sendMessage(value: String, topic: String): Unit = {
    val record = new ProducerRecord[String, String](topic, "", value)
    kafkaProducer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
          LOG.error(s"Error sending message: ${exception.getMessage}")
        } else {
          LOG.info(s"Message sent to topic ${metadata.topic()}," +
            s" partition ${metadata.partition()}, offset ${metadata.offset()}")
        }
      }
    })
    kafkaProducer.flush()
  }

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

  val schema = new Schema(
    List(
      Types.NestedField.required(1, "id", Types.LongType.get()),
      Types.NestedField.required(2, "name", Types.StringType.get()),
    ).asJava,
    Set(1).map(Int.box).asJava
  )

  private def createBucket(bucketName: String): Unit = {
    try {
      // Initialize the Minio client
      val minioClient = MinioClient.builder()
        .endpoint("http://localhost:9000") // MinIO server URL
        .credentials("minioadmin", "minioadmin") // Access key and secret key
        .build()

      // Create the bucket if it doesn't exist
      if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
        minioClient.makeBucket(
          MakeBucketArgs.builder().bucket(bucketName).build()
        )
        LOG.info(s"Bucket '$bucketName' created successfully!")
      } else {
        LOG.info(s"Bucket '$bucketName' already exists.")
      }

    } catch {
      case e: Throwable => LOG.error("Error occurred: " + e)
    }
  }

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
//    Thread.sleep(5000)
    createBucket("warehouse")
    // Create the table first
    val catalog = new DeltaCatalog()
    catalog.initialize(
      "delta-catalog",
      Map(
        CatalogProperties.CATALOG_IMPL -> classOf[DeltaCatalog].getCanonicalName,
        CatalogProperties.WAREHOUSE_LOCATION -> "s3a://warehouse/",
        CatalogProperties.FILE_IO_IMPL -> "org.apache.iceberg.aws.s3.S3FileIO",
        "s3.endpoint" -> s"http://localhost:9000",
        "s3.access-key-id" -> "minioadmin",
        "s3.secret-access-key" -> "minioadmin",
        "s3.path-style-access" -> "true",
        "client.region" -> "us-east-1"
      ).asJava
    )
    catalog.createTable(TableIdentifier.of("payments"), schema)
    LOG.info("Successfully create the table")
    startConnector()
    // Send message to topic
    sendMessage("""{"id": 1, "name": "peter"}""", "payments")


  }

  override def afterAll(): Unit = {
    stopContainer()
  }

  test("normal test") {
    Thread.sleep(10 * 60 * 1000)
  }
}
