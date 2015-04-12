package org.tribbloid.isparkserver.actors

/**
 * Created by peng on 12/29/14.
 */

import org.scalatest.FunSuite
import spray.http.StatusCodes._
import spray.routing.HttpService
import spray.testkit.ScalatestRouteTest

class TestISparkApiActor extends FunSuite with ScalatestRouteTest with HttpService {
  def actorRefFactory = system // connect the DSL to the test ActorSystem

  val smallRoute =
    get {
      pathSingleSlash {
        complete {
          <html>
            <body>
              <h1>Say hello to <i>spray</i>!</h1>
            </body>
          </html>
        }
      } ~
        path("ping") {
          complete("PONG!")
        }
    }

    test("put and get job") {
      Get() ~> smallRoute ~> check {
//        responseAs[String] must contain("Say hello")
      }
    }

    test("return a 'PONG!' response for GET requests to /ping") {
      Get("/ping") ~> smallRoute ~> check {
        responseAs[String] === "PONG!"
      }
    }

    test("leave GET requests to other paths unhandled") {
      Get("/kermit") ~> smallRoute ~> check {
//        handled must beFalse
      }
    }

    test("return a MethodNotAllowed error for PUT requests to the root path") {
      Put() ~> sealRoute(smallRoute) ~> check {
        status === MethodNotAllowed
        responseAs[String] === "HTTP method not allowed, supported methods: GET"
      }
    }
}