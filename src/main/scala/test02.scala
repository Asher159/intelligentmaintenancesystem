object test02 {
  def main(args: Array[String]): Unit = {
    var ints = List[(Int, Double)]((1,1.0),(2,1.0),(10,1.0),(20,1.0),(15,1.0))
    val tuples1 = ints.sortWith {
      (left, right) => left._1 + 0 < right._2 + 0
    }
    println(tuples1)
  }

}
