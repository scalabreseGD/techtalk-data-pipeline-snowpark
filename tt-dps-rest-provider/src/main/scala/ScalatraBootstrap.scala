import com.griddynamics.rest.servlet.PipelineServlet
import com.griddynamics.rest.{BaseApp, Bootstrap}

class ScalatraBootstrap extends Bootstrap {
  override def servlets() = Seq(new PipelineServlet)
}

object ScalatraBootstrap extends BaseApp {

  startServer()
}
