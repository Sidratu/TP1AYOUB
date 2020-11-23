package com.fakir

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Ex_2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    println("Exo2")

    val sparkSession = SparkSession.builder().master("local").getOrCreate()
    val df: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("data/donnees.csv")

    val renamed_df: DataFrame = df.withColumnRenamed("_c0", "nom_film")
      .withColumnRenamed("_c1", "nombre_vues")
      .withColumnRenamed("_c2", "note_film")
      .withColumnRenamed("_c3", "acteur_principal")

    val films_LeonardoDiCaprio: DataFrame = renamed_df.filter(renamed_df("acteur_principal") === "Di Caprio")
    print("Il y a " + films_LeonardoDiCaprio.count() + " films de Leonardo Di Caprio")

    val moyennes_des_notes_films_leonardo: DataFrame = films_LeonardoDiCaprio.groupBy( col1 = "acteur_principal").mean( colNames = "note_film")
    moyennes_des_notes_films_leonardo.show

    val TOTAL_des_vues_de_tout_les_films  = renamed_df.agg(sum("nombre_vues")).first.get(0).toString.toDouble
    val total_vues_films_ldc = films_LeonardoDiCaprio.agg(sum("nombre_vues")).first.get(0).toString.toDouble

    val pourcentage_vues_leonardoDicaprio: Double = total_vues_films_ldc / TOTAL_des_vues_de_tout_les_films * 100
    print("Le pourcentage des vues des films de Di Caprio est de " + pourcentage_vues_leonardoDicaprio + "%")

    val La_moyenne_notes_par_acteur = renamed_df.groupBy( col1 = "l'acteur_principal").mean( colNames = " Le_nombre_vues")
    La_moyenne_notes_par_acteur.show

    val La_moyenne_vues_par_acteur = renamed_df.groupBy( col1 = "L'acteur_principal").mean( colNames = "Le_note_film")
    La_moyenne_vues_par_acteur.show

    val pourcentage_vues = renamed_df.withColumn(colName = "pourcentage_de_vues", col(colName = "nombre_vues") / TOTAL_des_vues_de_tout_les_films * 100)
    pourcentage_vues.show

  }
}