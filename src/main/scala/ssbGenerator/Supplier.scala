package ssbGenerator

import scala.collection.mutable.HashMap

class Supplier {
  val  nation =Array ("ALGERIA","ARGENTINA","BRAZIL","CANADA","EGYPT","ETHIOPIA","FRANCE",
    "GERMANY","INDIA","INDONESIA","IRAN","IRAQ","JAPAN","JORDAN","KENYA","MOROCCO","MOZAMBIQUE","PERU","CHINA"
    ,"ROMANIA","SAUDI ARABIA","VIETNAM","RUSSIA","UNITED KINGDOM","UNITED STATES")

  def getSuppKey(suppkey:Int):Int={
    return suppkey+1
  }
  def getNameKey(key:Int,name:String) : String= {
    var numberAsString= "%09d".format(key)
    return name+"#"+numberAsString
  }
  def getAddress : String={
    val chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890, "

    var adr = ""
    var valeur=new Methodes()

    for( x<-0 to valeur.getValeurAleatoire(6, 25))
    {
      var i= (Math.floor(Math.random*63)).toInt
      adr += chars.charAt(i)
    }

    return adr
  }
  def getNation : String={

    var i = (Math.floor(Math.random() * 24)).toInt

    return  nation(i)
  }
  def getRegion(city:String) : String={
    val hashMap1: HashMap[String, String] = HashMap(("ALGERIA","AFRICA"),("ARGENTINA","AMERICA"),("BRAZIL","AMERICA"),
      ("CANADA","AMERICA"),("EGYPT","MIDDLE EAST"),("ETHIOPIA","AFRICA"),("FRANCE","EUROPE"),("GERMANY","EUROPE"),
      ("INDIA","ASIA"),("INDONESIA","ASIA"),("IRAN","MIDDLE EAST"),("IRAQ","MIDDLE EAST"),("JAPAN","ASIA"),
      ("JORDAN","MIDDLE EAST"),("KENYA","AFRICA"),("MOROCCO","AFRICA"),("MOZAMBIQUE","AFRICA"),("PERU","AMERICA"),
      ("CHINA","ASIA"),("ROMANIA","EUROPE"),("SAUDI ARABIA","MIDDLE EAST"),("VIETNAM","ASIA"),("RUSSIA","EUROPE"),
      ("UNITED KINGDOM","EUROPE"),("UNITED STATES","AMERICA"))

    return hashMap1(city)

  }
  def getCity(nati:String):String={
    var m=new Methodes
    var vnati =nati
    if(vnati.length()>=9){
      var city=vnati.substring(0, 9)
      return city+m.getValeurAleatoire(0, 9)
    }
    else{

      for( i<-(vnati.length+1) to 9){
        vnati = vnati + " "
      }
      return vnati+ m.getValeurAleatoire(0, 9)}}

  def getPhone(nat:String):String={
    var m=new Methodes
    var k=0
    while (!nat.equals(nation(k))&&(k< nation.length)){k=k+1}
    var valeur1 = 10+(k%25);
    var valeur2 = m.getValeurAleatoire(100, 999)
    var valeur3 = m.getValeurAleatoire(100, 999)
    var valeur4 = m.getValeurAleatoire(1000, 9999)

    return valeur1+"-"+valeur2+"-"+valeur3+"-"+valeur4
  }


}

