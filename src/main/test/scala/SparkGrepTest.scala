import org.scalatest.FunSuite

class SparkGrepTest extends FunSuite {

  test("Run the test") {
    SparkGrep.main(Array("local[*]", "src/main/scala/SparkGrep.scala", "val"))
  }

}
