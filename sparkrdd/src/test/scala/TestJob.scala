/*
 * @Description
 * @Author  mafei0728
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2020/9/17
 */
object TestJob extends App {
  val a: String = "10k-15k"
  println(a.replaceAll("[kK]","").split("-")(1))

}
