package org.eiennohito.crawl.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{Flow, Sink}
import org.scalatest.{BeforeAndAfterAll, FreeSpec, Matchers}
import akka.http.scaladsl.model.headers._
import akka.stream.ActorMaterializer
import org.apache.commons.io.IOUtils

import scala.concurrent.Await

/**
  * @author eiennohito
  * @since 2016/10/01
  */
class AkkaSpiderSpec extends FreeSpec with Matchers with BeforeAndAfterAll {

  protected val asys = ActorSystem("test")
  implicit protected val amat = ActorMaterializer.create(asys)



  "AkkaSpider" - {
    "works with ok page" in {
      def flow = Flow[HttpRequest].map { _ =>
        HttpResponse(
          headers = List[HttpHeader](
            `Content-Type`(ContentTypes.`text/html(UTF-8)`)
          ),
          entity = HttpEntity(
            IOUtils.toByteArray(getClass.getClassLoader.getResource("/sample.html"))
          )
        )
      }

      val pipeline = AkkaSpider.makePipeline(flow, amat)
      pipeline.runWith(Sink.seq)
    }
  }

  override protected def afterAll(): Unit = {
    import scala.concurrent.duration._
    Await.result(asys.terminate(), 1.second)
    super.afterAll()
  }
}
