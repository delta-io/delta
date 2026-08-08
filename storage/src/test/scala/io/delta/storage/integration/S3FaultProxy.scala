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

import java.io.{
  BufferedInputStream,
  BufferedOutputStream,
  ByteArrayOutputStream,
  EOFException,
  InputStream,
  OutputStream}
import java.net.{
  InetAddress,
  InetSocketAddress,
  ServerSocket,
  Socket,
  SocketException,
  URI}
import java.nio.charset.StandardCharsets
import java.util.Locale
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.util.control.NonFatal

/**
 * An HTTP fault proxy that drops selected successful S3 responses after the upstream operation
 * completes.
 *
 * The proxy forwards request bytes without rewriting headers because SigV4 includes the host and
 * signed headers. Each connection serves one HTTP request and then closes, which is valid for the
 * AWS SDK and keeps the fault injection independent of connection reuse.
 */
private[integration] final class S3FaultProxy(upstream: URI) extends AutoCloseable {
  import S3FaultProxy._

  require(upstream.getScheme == "http", "The S3 fault proxy requires an HTTP upstream endpoint.")
  require(
    upstream.getPath == null || upstream.getPath.isEmpty || upstream.getPath == "/",
    "The S3 fault proxy does not support an upstream path prefix.")

  private val requestTimeoutMillis = 120000
  private val running = new AtomicBoolean(true)
  private val dropPutResponse = new AtomicBoolean(true)
  private val dropMultipartCompleteResponse = new AtomicBoolean(true)
  private val injectPutConflict = new AtomicBoolean(true)
  private val injectMultipartCompleteConflict = new AtomicBoolean(true)
  private val putResponsesDropped = new AtomicInteger(0)
  private val multipartCompleteResponsesDropped = new AtomicInteger(0)
  private val putConflictsInjected = new AtomicInteger(0)
  private val multipartCompleteConflictsInjected = new AtomicInteger(0)
  private val lostPutHeadRequests = new AtomicInteger(0)
  private val lostMultipartCompleteHeadRequests = new AtomicInteger(0)
  private val putConflictHeadRequests = new AtomicInteger(0)
  private val multipartConflictHeadRequests = new AtomicInteger(0)
  private val putConflictPutRequests = new AtomicInteger(0)
  private val multipartConflictInitiateRequests = new AtomicInteger(0)
  private val multipartConflictCompleteRequests = new AtomicInteger(0)
  private val server = new ServerSocket()
  server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))

  private val daemonThreadFactory = new ThreadFactory {
    private val nextId = new AtomicInteger(0)

    override def newThread(runnable: Runnable): Thread = {
      val thread = new Thread(runnable, s"s3-fault-proxy-${nextId.incrementAndGet()}")
      thread.setDaemon(true)
      thread
    }
  }
  private val workers = Executors.newCachedThreadPool(daemonThreadFactory)
  private val acceptor = daemonThreadFactory.newThread(() => acceptConnections())
  acceptor.start()

  def endpoint: String = s"http://127.0.0.1:${server.getLocalPort}"

  def droppedPutResponseCount: Int = putResponsesDropped.get()

  def droppedMultipartCompleteResponseCount: Int = multipartCompleteResponsesDropped.get()

  def injectedPutConflictCount: Int = putConflictsInjected.get()

  def injectedMultipartCompleteConflictCount: Int = multipartCompleteConflictsInjected.get()

  def lostPutReconciliationHeadCount: Int = lostPutHeadRequests.get()

  def lostMultipartCompleteReconciliationHeadCount: Int =
    lostMultipartCompleteHeadRequests.get()

  def putConflictReconciliationHeadCount: Int = putConflictHeadRequests.get()

  def multipartConflictReconciliationHeadCount: Int = multipartConflictHeadRequests.get()

  def putConflictRequestCount: Int = putConflictPutRequests.get()

  def multipartConflictInitiateRequestCount: Int = multipartConflictInitiateRequests.get()

  def multipartConflictCompleteRequestCount: Int = multipartConflictCompleteRequests.get()

  private def acceptConnections(): Unit = {
    while (running.get()) {
      try {
        val client = server.accept()
        workers.execute(() => handle(client))
      } catch {
        case _: SocketException if !running.get() =>
        case NonFatal(failure) =>
          System.err.println(s"S3 fault proxy accept failed: ${failure.getMessage}")
      }
    }
  }

  private def handle(client: Socket): Unit = {
    var upstreamSocket: Socket = null
    try {
      client.setSoTimeout(requestTimeoutMillis)
      val clientInput = new BufferedInputStream(client.getInputStream)
      val clientOutput = new BufferedOutputStream(client.getOutputStream)
      val requestHeaderBytes = readHeader(clientInput)
      val requestHeader = parseHeader(requestHeaderBytes)
      recordRequest(requestHeader.startLine)

      if (requestHeader.headers.get("expect").exists(_.equalsIgnoreCase("100-continue"))) {
        // The proxy acknowledges Expect locally so the client can send the signed request body.
        clientOutput.write("HTTP/1.1 100 Continue\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1))
        clientOutput.flush()
      }

      if (claimSyntheticConflict(requestHeader.startLine)) {
        copyMessageBody(requestHeader, clientInput, DiscardingOutputStream)
        writeConditionalRequestConflict(clientOutput)
        return
      }

      upstreamSocket = new Socket()
      upstreamSocket.connect(
        new InetSocketAddress(upstream.getHost, upstreamPort),
        requestTimeoutMillis)
      upstreamSocket.setSoTimeout(requestTimeoutMillis)
      val upstreamInput = new BufferedInputStream(upstreamSocket.getInputStream)
      val upstreamOutput = new BufferedOutputStream(upstreamSocket.getOutputStream)
      upstreamOutput.write(requestHeaderBytes)
      upstreamOutput.flush()
      copyMessageBody(requestHeader, clientInput, upstreamOutput)
      upstreamOutput.flush()

      var responseHeaderBytes = readHeader(upstreamInput)
      var responseHeader = parseHeader(responseHeaderBytes)
      while (responseHeader.statusCode.contains(100)) {
        // The client already received the local 100 response, so consume the upstream copy.
        responseHeaderBytes = readHeader(upstreamInput)
        responseHeader = parseHeader(responseHeaderBytes)
      }

      val shouldDrop = responseHeader.statusCode.exists(code => code >= 200 && code < 300) &&
        claimFault(requestHeader.startLine)
      if (!shouldDrop) {
        clientOutput.write(responseHeaderBytes)
      }
      copyResponseBody(
        responseHeader,
        requestHeader.startLine.startsWith("HEAD "),
        upstreamInput,
        if (shouldDrop) DiscardingOutputStream else clientOutput)

      if (shouldDrop) {
        // SO_LINGER=0 turns close into a reset, matching a lost response after S3 committed it.
        client.setSoLinger(true, 0)
      } else {
        clientOutput.flush()
      }
    } catch {
      case _: EOFException =>
      case NonFatal(failure) if running.get() =>
        System.err.println(s"S3 fault proxy request failed: ${failure.getMessage}")
    } finally {
      closeQuietly(upstreamSocket)
      closeQuietly(client)
    }
  }

  private def upstreamPort: Int = {
    if (upstream.getPort >= 0) upstream.getPort else 80
  }

  private def claimFault(requestLine: String): Boolean = {
    val parts = requestLine.split(" ", 3)
    if (parts.length < 2) return false

    val method = parts(0)
    val target = parts(1)
    if (method == "PUT" && target.contains("s3-log-store-lost-put-response") &&
        dropPutResponse.compareAndSet(true, false)) {
      putResponsesDropped.incrementAndGet()
      System.err.println(s"S3 fault proxy dropped successful response for $requestLine")
      true
    } else if (method == "POST" &&
        target.contains("s3-log-store-lost-multipart-complete-response") &&
        target.contains("uploadId=") &&
        dropMultipartCompleteResponse.compareAndSet(true, false)) {
      multipartCompleteResponsesDropped.incrementAndGet()
      System.err.println(s"S3 fault proxy dropped successful response for $requestLine")
      true
    } else {
      false
    }
  }

  private def claimSyntheticConflict(requestLine: String): Boolean = {
    val parts = requestLine.split(" ", 3)
    if (parts.length < 2) return false

    val method = parts(0)
    val target = parts(1)
    if (method == "PUT" && target.contains("s3-log-store-409-put-retry") &&
        !target.contains("uploadId=") && injectPutConflict.compareAndSet(true, false)) {
      putConflictsInjected.incrementAndGet()
      System.err.println(s"S3 fault proxy injected 409 response for $requestLine")
      true
    } else if (method == "POST" &&
        target.contains("s3-log-store-409-multipart-complete-retry") &&
        target.contains("uploadId=") &&
        injectMultipartCompleteConflict.compareAndSet(true, false)) {
      multipartCompleteConflictsInjected.incrementAndGet()
      System.err.println(s"S3 fault proxy injected 409 response for $requestLine")
      true
    } else {
      false
    }
  }

  private def recordRequest(requestLine: String): Unit = {
    val parts = requestLine.split(" ", 3)
    if (parts.length < 2) return

    val method = parts(0)
    val target = parts(1)
    val targetPath = target.takeWhile(_ != '?')
    val isObjectHead = method == "HEAD" && !targetPath.endsWith("/")
    if (isObjectHead && target.contains("s3-log-store-lost-put-response")) {
      lostPutHeadRequests.incrementAndGet()
    } else if (isObjectHead &&
        target.contains("s3-log-store-lost-multipart-complete-response")) {
      lostMultipartCompleteHeadRequests.incrementAndGet()
    } else if (isObjectHead && target.contains("s3-log-store-409-put-retry")) {
      putConflictHeadRequests.incrementAndGet()
    } else if (isObjectHead &&
        target.contains("s3-log-store-409-multipart-complete-retry")) {
      multipartConflictHeadRequests.incrementAndGet()
    }

    if (method == "PUT" && target.contains("s3-log-store-409-put-retry") &&
        !target.contains("uploadId=")) {
      putConflictPutRequests.incrementAndGet()
    } else if (method == "POST" &&
        target.contains("s3-log-store-409-multipart-complete-retry") &&
        target.contains("uploads")) {
      multipartConflictInitiateRequests.incrementAndGet()
    } else if (method == "POST" &&
        target.contains("s3-log-store-409-multipart-complete-retry") &&
        target.contains("uploadId=")) {
      multipartConflictCompleteRequests.incrementAndGet()
    }
  }

  private def writeConditionalRequestConflict(output: OutputStream): Unit = {
    val body =
      "<Error><Code>ConditionalRequestConflict</Code>" +
        "<Message>Injected conditional request conflict.</Message>" +
        "<RequestId>delta-s3-log-store-test</RequestId></Error>"
    val bodyBytes = body.getBytes(StandardCharsets.UTF_8)
    val header =
      "HTTP/1.1 409 Conflict\r\n" +
        "Content-Type: application/xml\r\n" +
        s"Content-Length: ${bodyBytes.length}\r\n" +
        "Connection: close\r\n\r\n"
    output.write(header.getBytes(StandardCharsets.ISO_8859_1))
    output.write(bodyBytes)
    output.flush()
  }

  private def copyMessageBody(
      header: HttpHeader,
      input: InputStream,
      output: OutputStream): Unit = {
    header.contentLength match {
      case Some(length) => copyExactly(input, output, length)
      case None if header.isChunked => copyChunked(input, output)
      case None =>
    }
  }

  private def copyResponseBody(
      header: HttpHeader,
      isHeadRequest: Boolean,
      input: InputStream,
      output: OutputStream): Unit = {
    val statusHasNoBody = header.statusCode.exists(code => code == 204 || code == 304)
    if (isHeadRequest || statusHasNoBody) return

    header.contentLength match {
      case Some(length) => copyExactly(input, output, length)
      case None if header.isChunked => copyChunked(input, output)
      case None => copyUntilEnd(input, output)
    }
  }

  private def copyExactly(input: InputStream, output: OutputStream, length: Long): Unit = {
    val buffer = new Array[Byte](64 * 1024)
    var remaining = length
    while (remaining > 0) {
      val count = input.read(buffer, 0, math.min(buffer.length.toLong, remaining).toInt)
      if (count < 0) throw new EOFException(s"Expected $remaining additional HTTP body bytes.")
      output.write(buffer, 0, count)
      remaining -= count
    }
  }

  private def copyChunked(input: InputStream, output: OutputStream): Unit = {
    var complete = false
    while (!complete) {
      val sizeLine = readLine(input)
      output.write(sizeLine)
      val sizeText = new String(sizeLine, StandardCharsets.ISO_8859_1)
        .trim
        .takeWhile(_ != ';')
      val size = java.lang.Long.parseLong(sizeText, 16)
      if (size == 0) {
        var trailer = readLine(input)
        output.write(trailer)
        while (!java.util.Arrays.equals(trailer, CrLf)) {
          trailer = readLine(input)
          output.write(trailer)
        }
        complete = true
      } else {
        copyExactly(input, output, size)
        copyExactly(input, output, 2)
      }
    }
  }

  private def copyUntilEnd(input: InputStream, output: OutputStream): Unit = {
    val buffer = new Array[Byte](64 * 1024)
    var count = input.read(buffer)
    while (count >= 0) {
      output.write(buffer, 0, count)
      count = input.read(buffer)
    }
  }

  private def readHeader(input: InputStream): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    var matched = 0
    while (matched < HeaderTerminator.length) {
      val next = input.read()
      if (next < 0) throw new EOFException("Connection closed before the HTTP header completed.")
      bytes.write(next)
      matched = if (next == HeaderTerminator(matched)) matched + 1
        else if (next == HeaderTerminator(0)) 1
        else 0
      if (bytes.size() > 128 * 1024) {
        throw new IllegalArgumentException("HTTP header exceeded 128 KiB.")
      }
    }
    bytes.toByteArray
  }

  private def readLine(input: InputStream): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    var previous = -1
    var complete = false
    while (!complete) {
      val next = input.read()
      if (next < 0) throw new EOFException("Connection closed before the HTTP line completed.")
      bytes.write(next)
      complete = previous == '\r' && next == '\n'
      previous = next
    }
    bytes.toByteArray
  }

  private def parseHeader(bytes: Array[Byte]): HttpHeader = {
    val lines = new String(bytes, StandardCharsets.ISO_8859_1).split("\r\n")
    val headers = lines.drop(1).iterator.flatMap { line =>
      val separator = line.indexOf(':')
      if (separator < 0) None
      else Some(line.substring(0, separator).toLowerCase(Locale.ROOT) ->
        line.substring(separator + 1).trim)
    }.toMap
    HttpHeader(lines.head, headers)
  }

  private def closeQuietly(socket: Socket): Unit = {
    if (socket != null) {
      try socket.close()
      catch {
        case NonFatal(_) =>
      }
    }
  }

  override def close(): Unit = {
    if (running.compareAndSet(true, false)) {
      server.close()
      acceptor.join(TimeUnit.SECONDS.toMillis(5))
      workers.shutdownNow()
      workers.awaitTermination(5, TimeUnit.SECONDS)
    }
  }

  private case class HttpHeader(startLine: String, headers: Map[String, String]) {
    def contentLength: Option[Long] = headers.get("content-length").map(_.toLong)

    def isChunked: Boolean =
      headers.get("transfer-encoding").exists(_.toLowerCase(Locale.ROOT).contains("chunked"))

    def statusCode: Option[Int] = {
      if (!startLine.startsWith("HTTP/")) None
      else startLine.split(" ", 3).lift(1).map(_.toInt)
    }
  }

  private object DiscardingOutputStream extends OutputStream {
    override def write(value: Int): Unit = {}

    override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {}
  }
}

private object S3FaultProxy {
  private val HeaderTerminator = "\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1)
  private val CrLf = "\r\n".getBytes(StandardCharsets.ISO_8859_1)
}
