/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.storage.integration

import java.io.{ByteArrayOutputStream, IOException}
import java.net.{InetSocketAddress, URI, URLDecoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.{Base64, Locale}
import java.util.concurrent.{CountDownLatch, Executors, ThreadFactory, TimeUnit}

import scala.jdk.CollectionConverters._
import scala.collection.mutable

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.S3AFileSystem

object S3NativeTestFixture {
  // Track only resource lifetimes. Network operations remain the real S3A implementation.
  private val clients = mutable.Map.empty[String, mutable.Set[S3AFileSystem]]
  private[integration] def registerEndpoint(endpoint: String): Unit = clients.synchronized {
    clients(endpoint) = mutable.Set.empty
  }
  private[integration] def registerClient(endpoint: String, fs: S3AFileSystem): Unit = {
    clients.synchronized { clients.get(endpoint).foreach(_ += fs) }
  }
  private[integration] def closeClients(endpoint: String): Unit = {
    val owned = clients.synchronized { clients.remove(endpoint).toVector.flatMap(_.toVector) }
    var failure: IOException = null
    owned.foreach { fs =>
      try fs.close() catch {
        case e: IOException => if (failure == null) failure = e else failure.addSuppressed(e)
      }
    }
    if (failure != null) throw failure
  }

  val Put = "PUT"
  val Initiate = "INITIATE"
  val Part = "PART"
  val Complete = "COMPLETE"
  val Abort = "ABORT"
  val Head = "HEAD"
  val Get = "GET"
  val List = "LIST"
  val Copy = "COPY"
  val Delete = "DELETE_OBJECTS"

  final case class Request(operation: String, key: String, headers: Map[String, String],
      status: Int = 0, accepted: Boolean = false, query: Map[String, String] = Map.empty) {
    def conditional: Boolean = headers.get("if-none-match").contains("*")
  }

  /** Blocks only matching requests, outside the object-state lock. Always close in finally. */
  final class Gate private[integration] (
      val operation: String,
      val key: String,
      val afterAcceptance: Boolean,
      arrivals: Int) extends AutoCloseable {
    private val reached = new CountDownLatch(arrivals)
    private val released = new CountDownLatch(1)
    def await(timeoutSeconds: Long = 30): Boolean = reached.await(timeoutSeconds, TimeUnit.SECONDS)
    private[integration] def enter(): Unit = {
      reached.countDown()
      if (!released.await(60, TimeUnit.SECONDS)) {
        throw new IOException(s"Timed out at $operation/$key fixture gate")
      }
    }
    override def close(): Unit = released.countDown()
  }

  /** Fresh config, also used by independent JVM workers. Credentials are deliberately fake. */
  def configuration(endpoint: String): Configuration = {
    val conf = new Configuration(false)
    Map(
      "fs.s3a.impl" -> classOf[S3NativeTrackedFileSystem].getName,
      "fs.s3a.impl.disable.cache" -> "true",
      "fs.s3a.endpoint" -> endpoint,
      "fs.s3a.endpoint.region" -> "us-east-1",
      "fs.s3a.path.style.access" -> "true",
      "fs.s3a.connection.ssl.enabled" -> "false",
      "fs.s3a.aws.credentials.provider" -> "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
      "fs.s3a.access.key" -> "fixture-access-key",
      "fs.s3a.secret.key" -> "fixture-secret-key",
      "fs.s3a.bucket.probe" -> "0",
      "fs.s3a.retry.limit" -> "0",
      "fs.s3a.retry.throttle.limit" -> "0",
      "fs.s3a.retry.interval" -> "1ms",
      // Hadoop passes this as SDK numRetries, not total attempts (0 means one request).
      "fs.s3a.attempts.maximum" -> "1",
      "fs.s3a.connection.establish.timeout" -> "5s",
      "fs.s3a.connection.timeout" -> "15s",
      "fs.s3a.connection.request.timeout" -> "15s",
      "fs.s3a.connection.expect.continue" -> "false",
      "fs.s3a.change.detection.mode" -> "none",
      "fs.s3a.fast.upload.buffer" -> "array",
      "fs.s3a.multipart.size" -> "5M",
      "fs.s3a.multipart.threshold" -> "5M",
      "fs.s3a.directory.marker.retention" -> "keep"
    ).foreach { case (key, value) => conf.set(key, value) }
    conf
  }
}

/** Registers uncached S3A clients for fixture cleanup; never overrides any storage operation. */
class S3NativeTrackedFileSystem extends S3AFileSystem {
  override def initialize(uri: URI, conf: Configuration): Unit = {
    super.initialize(uri, conf)
    S3NativeTestFixture.registerClient(conf.get("fs.s3a.endpoint"), this)
  }
}

/**
 * In-memory, path-style S3 HTTP endpoint for real Hadoop S3A and AWS SDK integration tests.
 * Conditional publication and MPU completion are atomic under the same lock.
 * Faults and gates operate at the HTTP boundary, including dropped replies AFTER publication.
 *
 * This is deliberately not a general S3 emulator: it does not validate signatures, credentials,
 * checksums, regions, IAM, versioning, encryption, or service-side size limits.
 * It supports paginated ListObjects V1/V2, ranged GET, user metadata,
 * MPU operations, and basic copy/batch-delete for S3A rename.
 * Unknown operations fail instead of succeeding.
 * Production tests use actual S3A and SDK serialization, retries and exception translation.
 */
final class S3NativeTestFixture(
    settings: Map[String, String] = Map.empty) extends AutoCloseable {
  import S3NativeTestFixture._

  val bucket: String = "delta-native-test"
  private case class Stored(data: Array[Byte], metadata: Map[String, String], etag: String)
  private case class Upload(key: String, metadata: Map[String, String],
      parts: mutable.Map[Int, Stored] = mutable.Map.empty)
  private case class Fault(operation: String, key: String, status: Int, code: String,
      dropAfterAcceptance: Boolean)
  private val lock = new Object
  private val objects = mutable.Map.empty[String, Stored]
  private val uploads = mutable.Map.empty[String, Upload]
  private val history = mutable.ArrayBuffer.empty[Request]
  private val active = new java.util.IdentityHashMap[HttpExchange, Integer]()
  private val faults = mutable.ArrayBuffer.empty[Fault]
  private val gates = mutable.ArrayBuffer.empty[Gate]
  private val publications = mutable.Map.empty[String, Int].withDefaultValue(0)
  private val headFailures = mutable.Map.empty[String, (Int, String)]
  private val errors = mutable.ArrayBuffer.empty[Throwable]
  private var nextUpload = 0L
  private val executor = Executors.newCachedThreadPool(new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val thread = new Thread(r, "s3-native-fixture")
      thread.setDaemon(true)
      thread
    }
  })
  private val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
  server.setExecutor(executor)
  server.createContext("/", new HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      try handleRequest(exchange) catch {
        // Client disconnects are expected when the worker is killed or a reply is dropped.
        case _: IOException => exchange.close()
        case e: Throwable =>
          lock.synchronized { errors += e }
          try error(exchange, 500, "FixtureError") finally exchange.close()
      } finally { lock.synchronized { active.remove(exchange) } }
    }
  })
  server.start()
  registerEndpoint(endpoint)

  def endpoint: String = s"http://127.0.0.1:${server.getAddress.getPort}"
  def configuration: Configuration = {
    val conf = S3NativeTestFixture.configuration(endpoint)
    settings.foreach { case (key, value) => conf.set(key, value) }
    conf
  }
  def path(key: String): Path = new Path(s"s3a://$bucket/$key")
  def requests: Vector[Request] = lock.synchronized { history.toVector }
  def handlerErrors: Vector[Throwable] = lock.synchronized { errors.toVector }
  def bytes(key: String): Option[Array[Byte]] = lock.synchronized {
    objects.get(key).map(_.data.clone())
  }
  def metadata(key: String): Map[String, String] = lock.synchronized {
    objects.get(key).map(_.metadata).getOrElse(Map.empty)
  }
  def acceptedPublications(key: String): Int = lock.synchronized { publications(key) }
  def pendingUploads: Int = lock.synchronized { uploads.size }
  def seed(key: String, bytes: Array[Byte], metadata: Map[String, String] = Map.empty): Unit = {
    lock.synchronized { objects(key) = stored(bytes.clone(), metadata) }
  }
  def failNext(operation: String, key: String, status: Int, code: String,
      times: Int = 1): Unit = lock.synchronized {
    faults ++= Vector.fill(times)(Fault(operation, key, status, code, false))
  }
  def loseNextResponse(operation: String, key: String, times: Int = 1): Unit = {
    require(operation == Put || operation == Complete)
    lock.synchronized { faults ++= Vector.fill(times)(Fault(operation, key, 0, "", true)) }
  }
  /** HEAD starts failing only once this key has actually been published. */
  def failHeadAfterAcceptance(key: String, status: Int = 503,
      code: String = "ServiceUnavailable"): Unit = lock.synchronized {
    headFailures(key) = status -> code
  }
  def pause(operation: String, key: String, afterAcceptance: Boolean = false,
      arrivals: Int = 1): Gate = lock.synchronized {
    require(arrivals > 0)
    require(!afterAcceptance || operation == Put || operation == Complete)
    val gate = new Gate(operation, key, afterAcceptance, arrivals)
    gates += gate
    gate
  }
  override def close(): Unit = {
    lock.synchronized { gates.foreach(_.close()) }
    try closeClients(endpoint) finally {
      server.stop(0)
      executor.shutdownNow()
      executor.awaitTermination(5, TimeUnit.SECONDS)
    }
  }

  private def elements(xml: Array[Byte], tag: String): Vector[String] = {
    val factory = javax.xml.parsers.DocumentBuilderFactory.newInstance()
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
    factory.setFeature("http://xml.org/sax/features/external-general-entities", false)
    factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false)
    val document = factory.newDocumentBuilder().parse(new java.io.ByteArrayInputStream(xml))
    val nodes = document.getElementsByTagName(tag)
    (0 until nodes.getLength).map(i => nodes.item(i).getTextContent).toVector
  }

  private def stored(data: Array[Byte], metadata: Map[String, String]): Stored = {
    val digest = MessageDigest.getInstance("MD5").digest(data)
      .map(b => f"${b & 0xff}%02x").mkString
    Stored(data, metadata, "\"" + digest + "\"")
  }
  private def escape(s: String): String = s.replace("&", "&amp;").replace("<", "&lt;")
    .replace(">", "&gt;").replace("\"", "&quot;").replace("'", "&apos;")
  private def decode(s: String): String = URLDecoder.decode(s, "UTF-8")
  private def xml(exchange: HttpExchange, status: Int, body: String): Unit = {
    exchange.getResponseHeaders.set("Content-Type", "application/xml")
    reply(exchange, status, body.getBytes(UTF_8))
  }
  private def reply(exchange: HttpExchange, status: Int, data: Array[Byte]): Unit = {
    lock.synchronized {
      Option(active.get(exchange)).foreach { index =>
        history(index.intValue) = history(index.intValue).copy(status = status)
      }
    }
    exchange.getResponseHeaders.set("x-amz-request-id", "fixture-request")
    if (exchange.getRequestMethod == "HEAD" || status == 204) {
      exchange.sendResponseHeaders(status, -1)
    } else {
      exchange.sendResponseHeaders(status, if (data.isEmpty) -1 else data.length.toLong)
      if (data.nonEmpty) exchange.getResponseBody.write(data)
    }
    exchange.close()
  }
  private def error(exchange: HttpExchange, status: Int, code: String): Unit = {
    exchange.getResponseHeaders.set("x-amz-error-code", code)
    xml(exchange, status, s"<Error><Code>$code</Code><Message>$code</Message>" +
      "<RequestId>fixture-request</RequestId></Error>")
  }
  private def waitAt(operation: String, key: String, after: Boolean): Unit = {
    val matching = lock.synchronized {
      gates.filter(g => g.operation == operation && g.key == key &&
        g.afterAcceptance == after).toVector
    }
    matching.foreach(_.enter())
  }
  private def body(exchange: HttpExchange, headers: Map[String, String]): Array[Byte] = {
    val out = new ByteArrayOutputStream
    val buffer = new Array[Byte](8192)
    val in = exchange.getRequestBody
    var read = in.read(buffer)
    while (read != -1) { out.write(buffer, 0, read); read = in.read(buffer) }
    val raw = out.toByteArray
    if (!headers.getOrElse("content-encoding", "").contains("aws-chunked")) return raw
    // HttpServer removes HTTP transfer chunking; AWS streaming-signature/checksum framing remains.
    val decoded = new ByteArrayOutputStream
    var pos = 0
    var done = false
    while (!done) {
      var end = pos
      while (end + 1 < raw.length && !(raw(end) == 13 && raw(end + 1) == 10)) end += 1
      require(end + 1 < raw.length, "Incomplete AWS chunk header")
      val size = Integer.parseInt(new String(raw, pos, end - pos, UTF_8).takeWhile(_ != ';'), 16)
      pos = end + 2
      if (size == 0) done = true else {
        require(size <= raw.length - pos - 2, "Incomplete AWS chunk")
        decoded.write(raw, pos, size)
        pos += size + 2
      }
    }
    decoded.toByteArray
  }

  private def handleRequest(exchange: HttpExchange): Unit = {
    val uri = exchange.getRequestURI
    val path = uri.getPath.stripPrefix("/")
    if (path.takeWhile(_ != '/') != bucket) { error(exchange, 404, "NoSuchBucket"); return }
    val key = path.drop(bucket.length).stripPrefix("/")
    val query = Option(uri.getRawQuery).toSeq.flatMap(_.split("&")).map { item =>
      val pair = item.split("=", 2)
      decode(pair(0)) -> (if (pair.length == 2) decode(pair(1)) else "")
    }.toMap
    val headers = exchange.getRequestHeaders.asScala.map { case (k, v) =>
      k.toLowerCase(Locale.ROOT) -> v.asScala.mkString(",")
    }.toMap
    val method = exchange.getRequestMethod
    val operation = (method, query.contains("uploads"), query.contains("uploadId")) match {
      case ("PUT", _, _) if headers.contains("x-amz-copy-source") => Copy
      case ("POST", _, _) if query.contains("delete") => Delete
      case ("POST", true, _) => Initiate
      case ("PUT", _, true) => Part
      case ("POST", _, true) => Complete
      case ("DELETE", _, true) => Abort
      case ("GET", _, _) if key.isEmpty => List
      case _ => method
    }
    lock.synchronized {
      active.put(exchange, history.size)
      history += Request(operation, key, headers, query = query)
    }
    waitAt(operation, key, after = false)
    val fault = lock.synchronized {
      val index = faults.indexWhere(f => f.operation == operation && f.key == key)
      if (index < 0) None else Some(faults.remove(index))
    }
    if (fault.exists(!_.dropAfterAcceptance)) {
      val f = fault.get
      if (f.code == "NoSuchUpload") lock.synchronized { uploads.remove(query("uploadId")) }
      error(exchange, f.status, f.code)
      return
    }
    val probeFailure = lock.synchronized {
      if (operation == Head && publications(key) > 0) headFailures.get(key) else None
    }
    if (probeFailure.nonEmpty) {
      error(exchange, probeFailure.get._1, probeFailure.get._2)
      return
    }
    val metadata = headers.collect { case (k, v) if k.startsWith("x-amz-meta-") =>
      k.stripPrefix("x-amz-meta-") -> v
    }
    operation match {
      case Head if key.isEmpty => reply(exchange, 200, Array.emptyByteArray)
      case Head | Get =>
        lock.synchronized { objects.get(key) } match {
          case None => error(exchange, 404, "NoSuchKey")
          case Some(value) =>
            val response = exchange.getResponseHeaders
            response.set("ETag", value.etag)
            response.set("Last-Modified", "Thu, 01 Jan 2026 00:00:00 GMT")
            response.set("Content-Type", "application/octet-stream")
            value.metadata.foreach { case (k, v) => response.set("x-amz-meta-" + k, v) }
            val range = headers.get("range").filter(_ => operation == Get)
            val (start, end) = range.map { r =>
              val pieces = r.stripPrefix("bytes=").split("-", -1)
              pieces(0).toInt -> (if (pieces(1).isEmpty) value.data.length - 1
                else math.min(pieces(1).toInt, value.data.length - 1))
            }.getOrElse(0 -> (value.data.length - 1))
            val data = value.data.slice(start, end + 1)
            response.set("Content-Length", data.length.toString)
            if (range.nonEmpty) response.set("Content-Range",
              s"bytes $start-$end/${value.data.length}")
            reply(exchange, if (range.nonEmpty) 206 else 200, data)
        }
      case Copy =>
        val source = decode(headers("x-amz-copy-source").stripPrefix("/"))
          .stripPrefix(bucket + "/")
        val copied = lock.synchronized {
          objects.get(source).map { original =>
            val value = if (headers.get("x-amz-metadata-directive").contains("REPLACE")) {
              original.copy(metadata = metadata)
            } else original
            objects(key) = value
            value
          }
        }
        copied match {
          case None => error(exchange, 404, "NoSuchKey")
          case Some(value) => xml(exchange, 200,
            "<CopyObjectResult><LastModified>2026-01-01T00:00:00.000Z</LastModified>" +
              s"<ETag>${escape(value.etag)}</ETag></CopyObjectResult>")
        }
      case Delete =>
        val keys = elements(body(exchange, headers), "Key")
        lock.synchronized { keys.foreach(objects.remove) }
        xml(exchange, 200, "<DeleteResult>" + keys.map(k =>
          s"<Deleted><Key>${escape(k)}</Key></Deleted>").mkString + "</DeleteResult>")
      case Put =>
        val value = stored(body(exchange, headers), metadata)
        publish(exchange, operation, key, headers, value, fault, None)
      case Initiate =>
        val id = lock.synchronized {
          nextUpload += 1
          val id = s"upload-$nextUpload"
          uploads(id) = Upload(key, metadata)
          id
        }
        xml(exchange, 200, s"<InitiateMultipartUploadResult><Bucket>$bucket</Bucket>" +
          s"<Key>${escape(key)}</Key><UploadId>$id</UploadId></InitiateMultipartUploadResult>")
      case Part =>
        val value = stored(body(exchange, headers), Map.empty)
        val exists = lock.synchronized {
          uploads.get(query("uploadId")).filter(_.key == key).exists { upload =>
            upload.parts(query("partNumber").toInt) = value
            true
          }
        }
        if (!exists) error(exchange, 404, "NoSuchUpload") else {
          exchange.getResponseHeaders.set("ETag", value.etag)
          reply(exchange, 200, Array.emptyByteArray)
        }
      case Complete =>
        val request = body(exchange, headers)
        val numbers = elements(request, "PartNumber").map(_.toInt)
        val etags = elements(request, "ETag")
        val upload = lock.synchronized { uploads.get(query("uploadId")) }
        if (upload.isEmpty || upload.get.key != key) error(exchange, 404, "NoSuchUpload")
        else if (numbers.isEmpty || numbers != numbers.sorted.distinct ||
            !numbers.forall(upload.get.parts.contains) ||
            etags != numbers.map(n => upload.get.parts(n).etag)) {
          error(exchange, 400, "InvalidPart")
        }
        else {
          val u = upload.get
          val data = numbers.flatMap(n => u.parts(n).data).toArray
          publish(exchange, operation, key, headers, stored(data, u.metadata), fault,
            Some(query("uploadId")))
        }
      case Abort =>
        lock.synchronized { uploads.remove(query("uploadId")) }
        reply(exchange, 204, Array.emptyByteArray)
      case List => list(exchange, query)
      case "DELETE" =>
        lock.synchronized { objects.remove(key) }
        reply(exchange, 204, Array.emptyByteArray)
      case _ => error(exchange, 501, "NotImplemented")
    }
  }

  private def publish(exchange: HttpExchange, operation: String, key: String,
      headers: Map[String, String], value: Stored, fault: Option[Fault],
      uploadId: Option[String]): Unit = {
    val accepted = lock.synchronized {
      if (headers.get("if-none-match").contains("*") && objects.contains(key)) false
      else {
        objects(key) = value
        val index = active.get(exchange).intValue
        history(index) = history(index).copy(accepted = true)
        publications(key) += 1
        uploadId.foreach(uploads.remove)
        true
      }
    }
    if (!accepted) { error(exchange, 412, "PreconditionFailed"); return }
    waitAt(operation, key, after = true)
    if (fault.exists(_.dropAfterAcceptance)) { exchange.close(); return }
    exchange.getResponseHeaders.set("ETag", value.etag)
    if (operation == Complete) {
      xml(exchange, 200, s"<CompleteMultipartUploadResult><Location>$endpoint/$bucket/" +
        s"${escape(key)}</Location><Bucket>$bucket</Bucket><Key>${escape(key)}</Key>" +
        s"<ETag>${escape(value.etag)}</ETag></CompleteMultipartUploadResult>")
    } else reply(exchange, 200, Array.emptyByteArray)
  }

  private def list(exchange: HttpExchange, query: Map[String, String]): Unit = {
    val prefix = query.getOrElse("prefix", "")
    val delimiter = query.getOrElse("delimiter", "")
    val v2 = query.get("list-type").contains("2")
    val after = query.get("continuation-token").map { token =>
      new String(Base64.getUrlDecoder.decode(token), UTF_8)
    }.getOrElse(query.getOrElse("start-after", query.getOrElse("marker", "")))
    val entries = lock.synchronized { objects.toVector.filter(_._1.startsWith(prefix)) }
    // Collapse prefixes before filtering and paging so a directory occupies one result slot,
    // even when it has many children, and never reappears on subsequent pages.
    val grouped = entries.map { case (key, value) =>
      val index = if (delimiter.isEmpty) -1 else key.indexOf(delimiter, prefix.length)
      if (index >= 0) key.substring(0, index + delimiter.length) -> None
      else key -> Some(value)
    }.toMap.toVector.filter(_._1 > after).sortBy(_._1)
    val maxKeys = math.min(query.getOrElse("max-keys", "1000").toInt, 1000)
    require(maxKeys >= 0, "max-keys must be nonnegative")
    val page = grouped.take(maxKeys)
    val truncated = page.nonEmpty && grouped.size > page.size
    val contents = page.collect { case (key, Some(value)) =>
      s"<Contents><Key>${escape(key)}</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified>" +
        s"<ETag>${escape(value.etag)}</ETag><Size>${value.data.length}</Size>" +
        "<StorageClass>STANDARD</StorageClass></Contents>"
    }.mkString
    val prefixes = page.collect { case (key, None) =>
      s"<CommonPrefixes><Prefix>${escape(key)}</Prefix></CommonPrefixes>"
    }.mkString
    val next = if (!truncated) "" else if (v2) {
      val token = Base64.getUrlEncoder.withoutPadding().encodeToString(page.last._1.getBytes(UTF_8))
      s"<NextContinuationToken>$token</NextContinuationToken>"
    } else s"<NextMarker>${escape(page.last._1)}</NextMarker>"
    val cursor = if (v2) {
      query.get("continuation-token").map(t =>
        s"<ContinuationToken>${escape(t)}</ContinuationToken>").getOrElse("") +
        query.get("start-after").map(k => s"<StartAfter>${escape(k)}</StartAfter>").getOrElse("")
    } else s"<Marker>${escape(query.getOrElse("marker", ""))}</Marker>"
    xml(exchange, 200, s"<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
      s"<Name>$bucket</Name><Prefix>${escape(prefix)}</Prefix><MaxKeys>$maxKeys</MaxKeys>" +
      s"<Delimiter>${escape(delimiter)}</Delimiter><KeyCount>${page.size}</KeyCount>" +
      s"<IsTruncated>$truncated</IsTruncated>" + cursor + next +
      contents + prefixes + "</ListBucketResult>")
  }
}
