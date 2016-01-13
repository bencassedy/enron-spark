import com.bencassedy.spartakos.common.SpartakosSparkContext
import org.apache.spark.ml.PipelineModel
import com.bencassedy.spartakos.utils.StringUtils

/**
  * Test case to load model from file
  */
object TestLoadModel extends App {
  val (sparkContext, sqlContext) = SpartakosSparkContext.init

  val model = sparkContext
    .objectFile[PipelineModel]("src/main/resources/glm.model").first()

  println(model.explainParams())
}
