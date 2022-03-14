import com.griddynamics.rest.{Bootstrap, RestServlet}

class ScalatraBootstrap extends Bootstrap {
  override def servlets() = Seq(new RestServlet)
}

object ScalatraBootstrap extends BaseApp {
  startServer()
}