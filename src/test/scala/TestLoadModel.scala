import com.bencassedy.enron.common.EnronSparkContext
import org.apache.spark.ml.PipelineModel
import com.bencassedy.enron.utils.EnronUtils

/**
  * Test case to load model from file
  */
object TestLoadModel extends App {
  val (sparkContext, sqlContext) = EnronSparkContext.init

  val model = sparkContext
    .objectFile[PipelineModel]("src/main/resources/glm.model").first()

  println(model.explainParams())
}
