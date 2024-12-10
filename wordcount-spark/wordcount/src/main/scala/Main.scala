import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // Charger le fichier texte
    val filePath = "../../lorem.txt"
    val textFile = sc.textFile(filePath)

    // Transformation
    val wordCounts = textFile
      .flatMap(_.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    // Afficher les résultats
    wordCounts.collect().foreach(println)

    spark.stop()
  }
}
