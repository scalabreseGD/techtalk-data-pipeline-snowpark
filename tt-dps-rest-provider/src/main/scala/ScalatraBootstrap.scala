import com.griddynamics.rest.servlet.RestServlet
import com.griddynamics.rest.{BaseApp, Bootstrap}

class ScalatraBootstrap extends Bootstrap {
  override def servlets() = Seq(new RestServlet)
}

object ScalatraBootstrap extends BaseApp {


  startServer()
}
