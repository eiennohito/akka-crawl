package org.eiennohito.crawl.http

import java.nio.ByteBuffer

import org.apache.commons.io.IOUtils
import org.jsoup.parser.Parser
import org.scalatest.{FreeSpec, Matchers}

/**
  * @author eiennohito
  * @since 2016/10/02
  */
class LangSpec extends FreeSpec with Matchers {

  def checkJp(bytes: Array[Byte], charset: Option[String]) = {
    val doc = HttpParserStage.parseDocument(bytes.length, Parser.htmlParser(), ByteBuffer.wrap(bytes), charset, "http://none")
    doc.lang shouldBe Some("ja")
  }

  "Language Is Japanese" - {
    "in page1" in {
      val src = IOUtils.toByteArray(getClass.getClassLoader.getResourceAsStream("jp/p1.html"))
      checkJp(src, None)
    }

    "in page2" in {
      val src = IOUtils.toByteArray(getClass.getClassLoader.getResourceAsStream("jp/p2.html"))
      checkJp(src, Some("UTF-8"))
    }

    "in page3" in {
      val src = IOUtils.toByteArray(getClass.getClassLoader.getResourceAsStream("jp/p3.html"))
      checkJp(src, None)
    }
  }
}
