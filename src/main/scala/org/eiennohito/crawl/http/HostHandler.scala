package org.eiennohito.crawl.http

import java.io.InputStream
import java.nio.ByteBuffer

import akka.http.scaladsl.model.Uri
import com.typesafe.scalalogging.StrictLogging
import crawlercommons.robots.SimpleRobotRules.RobotRulesMode
import crawlercommons.robots.{BaseRobotRules, SimpleRobotRules, SimpleRobotRulesParser}
import org.apache.commons.io.IOUtils

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

object HostHandler extends StrictLogging {
  case class Completed(base: Uri, data: Set[Uri.Path])
  case class Skipped(base: Uri)
  case object Retry
  case object HandleRobots
  case object RobotsUnavailable
  case object MaybeComplete

  def parseRobots(doc: ProcessedDocument, botName: String): Option[BaseRobotRules] = {
    parseRobots(
      doc.uri.toString(),
      doc.doc.bytes.array(),
      botName,
      doc.headers.find(_.is("content-type")).map(_.value()).orNull
    )
  }

  def parseRobots(uri: String, data: Array[Byte], robotName: String, ctype: String): Option[BaseRobotRules] = {
    try {
      val parser = new SimpleRobotRulesParser
      Some(parser.parseContent(uri, data, ctype, robotName))
    } catch {
      case e: Exception =>
        logger.debug(s"could not parse robots.txt for $uri", e)
        None
    }
  }

  def makeFilter(uri: String, content: Array[Byte], ctype: String, name: String) = {
    val parser = new SimpleRobotRulesParser
    val rules = parser.parseContent(uri, content, ctype, name)
    try {
      new UrlFilter(rules)
    } catch {
      case e: Exception =>
        logger.debug(s"could not parse robots.txt for $uri, forbidding crawl")
        allowNone
    }
  }

  def getBots(uri: Uri) = {
    scala.concurrent.blocking {
      try {
        val url = new java.net.URL(uri.toString())
        val input = url.openConnection()
        input.connect()

        var stream: InputStream = null
        val bytes = try {
          stream = input.getInputStream
          IOUtils.toByteArray(stream)
        } finally {
          stream.close()
        }
        Some(ProcessedDocument(
          uri,
          ParsedDocument(
            bytes.length,
            ByteBuffer.wrap(bytes),
            Failure(new Exception),
            None,
            None
          ),
          Nil
        ))
      } catch {
        case e: Exception => None
      }
    }
  }

  val allowNone = new UrlFilter(new SimpleRobotRules(RobotRulesMode.ALLOW_NONE))
  val allowAll = new UrlFilter(new SimpleRobotRules(RobotRulesMode.ALLOW_ALL))
}
