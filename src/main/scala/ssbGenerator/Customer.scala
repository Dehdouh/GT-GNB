package ssbGenerator

class Customer {

  def  getCustKey(custkey:Int):Int={
    return custkey+1
  }
  def getMKTSEGMENT : String={
    val proprietes=Array("AUTOMOBILE","BUILDING","FURNITURE","MACHINERY","HOUSEHOLD" )
    var valeur=new Methodes
    return proprietes(valeur.getValeurAleatoire(0, 5))
  }
}
