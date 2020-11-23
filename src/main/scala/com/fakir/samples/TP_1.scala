package com.fakir

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TP1 {



  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    println("Hello world")

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //Question 1
    val rdd = sparkSession.sparkContext.textFile("data/donnees.csv")

    //Question 2
    val films_leonardoDC = rdd.filter(elem => elem.contains("Di Caprio"))
    print("Il y a " + films_leonardoDC.count() + " films de Leonardo Di Caprio" + "\n")

    //Question 3
    val Notes_leonardo = films_leonardoDC.map(item => (item.split(";")(2).toDouble))
    val moyenne = Notes_leonardo.sum
    print("La moyenne des films de Di Caprio est: " + moyenne / films_leonardoDC.count() + "\n")

    //Question 4
    val spectateur_leo = films_leonardoDC.map(item => (item.split(";")(1).toDouble))
    val toutlesvues = rdd.map(item => (item.split(";")(1).toDouble))
    val notes_spectateur_leo = spectateur_leo.sum() / toutlesvues.sum()
    println("Le pourcentage des personnes qui regarde les films de Di Caprio: " + notes_spectateur_leo * 100 + "%")
    //Question 5
    val acteurs = rdd.map(item => (item.split(";")(3))).distinct.collect()
    acteurs.foreach(println)


  }
  }
