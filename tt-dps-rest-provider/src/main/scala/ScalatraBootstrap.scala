import com.griddynamics.rest.{BaseApp, Bootstrap, RestServlet}

class ScalatraBootstrap extends Bootstrap {
  override def servlets() = Seq(new RestServlet)
}

object ScalatraBootstrap extends BaseApp {


  startServer()

  override def basePath() = "/meow"

  override def port() = 8080
}
