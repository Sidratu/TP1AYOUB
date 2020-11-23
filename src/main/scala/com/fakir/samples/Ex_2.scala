package com.fakir

/* Partie 2 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Ex_2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    println("Test partie2")

    val sparkSession = SparkSession.builder().master("local").getOrCreate()
    val df: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("data/donneesMOD1.csv")

    val renamed_df: DataFrame = df.withColumnRenamed("_c0", "nom_film")
      .withColumnRenamed("_c1", "nombre_vues")
      .withColumnRenamed("_c2", "note_film")
      .withColumnRenamed("_c3", "acteur_principal")

    val films_leonardo: DataFrame = renamed_df.filter(renamed_df("acteur_principal") === "Di Caprio")
    print("Il y a " + films_leonardo.count() + " films de Leonardo Di Caprio")

    val moy_notes_films_leonardo: DataFrame = films_leonardo.groupBy( col1 = "acteur_principal").mean( colNames = "note_film")
    moy_notes_films_leonardo.show

    val total_vues_films  = renamed_df.agg(sum("nombre_vues")).first.get(0).toString.toDouble
    val total_vues_films_leonardo = films_leonardo.agg(sum("nombre_vues")).first.get(0).toString.toDouble

    val pourcentage_vues_ldc: Double = total_vues_films_leonardo / total_vues_films * 100
    print("Le pourcentage des vues des films de Di Caprio est de " + pourcentage_vues_ldc)

    val moyenne_notes_par_acteur = renamed_df.groupBy( col1 = "acteur_principal").mean( colNames = "nombre_vues")
    moyenne_notes_par_acteur.show

    val moyenne_vues_par_acteur = renamed_df.groupBy( col1 = "acteur_principal").mean( colNames = "note_film")
    moyenne_vues_par_acteur.show

    val pourcentage_vues = renamed_df.withColumn(colName = "pourcentage_de_vues", col(colName = "nombre_vues") / total_vues_films * 100)
    pourcentage_vues.show

  }
}