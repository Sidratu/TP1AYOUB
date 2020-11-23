package com.fakir

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TP1_exo1 {



  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    println("Hello world")

    val sparkSession = SparkSession.builder().master("local").getOrCreate()


    val rdd = sparkSession.sparkContext.textFile("data/donneesMOD1.csv")

    val films_leonardo = rdd.filter(elem => elem.contains("Di Caprio"))
    print("Il y a " + films_leonardo.count() + " films de Leonardo Di Caprio" + '\n')

    /* QUESTION 3 */

    val counts = films_leonardo.map(item => (item.split(";")(2).toDouble) )
    counts.foreach(println)

    val moyenne = counts.sum
    println("la moyennne des films de Di Caprio" + " " + moyenne/films_leonardo.count())

    /* Question 4 */
    val visionnagedesfilmsLeonardo = films_leonardo.map(item => (item.split(";")(1).toDouble))
    val visionnage = rdd.map(item => (item.split(";")(1).toDouble))
    val pourcentagefilmLeonardo = visionnagedesfilmsLeonardo.sum() / visionnage.sum()
    println(pourcentagefilmLeonardo*100)

    /* question 5 */

    val acteurs = rdd.map(item => (item.split(";")(3)) ).distinct.collect()
    acteurs.foreach(println)


  }
}