import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions => f}

/*
 * @Description
 * @Author  mafei0728
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2020/10/12
 */
object SparkTest extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("demo01").setMaster("local[2]")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  case class User(id: Int, line: String)

  val staticDate = Seq((1, "1,ma"), (2, "2,liu"))
  val staticDF = spark.createDataFrame(staticDate.map(x => User(x._1, x._2)))
  staticDF.show()
  staticDF.withColumn("x", f.split(f.col("line"),",")(0)).show()
}
