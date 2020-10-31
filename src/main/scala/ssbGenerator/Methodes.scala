package ssbGenerator

import scala.annotation.switch
import scala.util.Random

class Methodes {
  def getValeurAleatoire(min: Int, max: Int): Int = {
    var r = new Random()
    var valeur = min + r.nextInt(max - min)
    return valeur
  }

  def log2(a: Double): Double = {
    return Math.log(a) / Math.log(2)
  }

  def getExtension(fichierSortie: String): String = {
    val extension = (fichierSortie: @switch) match {
      case "csv" =>".csv"
      case "tbl" => ".tbl"
      case "json" =>".json"
    }
    return extension
  }

  def getSeparateur(fichierSortie: String): String = {
    val separateur = (fichierSortie: @switch) match {
      case "csv" =>";"
      case "tbl" => "|"
      case "json"=>" "

    }
    return separateur
  }
}


