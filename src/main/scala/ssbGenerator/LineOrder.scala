package ssbGenerator

import java.lang.Math.floor
import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}
import java.util

import scala.util.Random

class LineOrder {
  def getCUSTKEY(sf:Double):Int={
    var valeur=new Methodes
    var x= valeur.getValeurAleatoire(1, (sf*30000).toInt);
    if(x%3==0){
      getCUSTKEY(sf);
      return x;
    }
    else{
      return x;
    }

  }
  def getSUPPKEY(sf:Double):Int={
    var valeur=new Methodes
    return valeur.getValeurAleatoire(1,(sf*2000).toInt);
  }
  def getPARTKEY(sf:Double):Int={
    var valeur=new Methodes
    var suppkey=200000*(floor(1+valeur.log2(sf))).toInt
    return valeur.getValeurAleatoire(1, suppkey)
  }
  def getORDERDATE:String={
    val random = new Random()
    val formater =new SimpleDateFormat("yyyyMMdd");
    var minDay = (LocalDate.of(1900, 1, 1).toEpochDay()).toInt
    var maxDay = (LocalDate.of(2015, 1, 1).toEpochDay()).toInt
    var randomDay = (minDay + random.nextInt(maxDay - (minDay-151))).toLong

    var orderDate = LocalDate.ofEpochDay(randomDay)
    var date=util.Date.from(orderDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());


    return formater.format(date);
  }
  def  getORDERPRIORITY:String={
    val proprietes = Array("1-URGENT","2-HIGH","3-MEDIUM","4-NOT SPECIFIED" )
    var valeur=new Methodes
    return proprietes(valeur.getValeurAleatoire(0, 4))

  }
  def getSHIPPRIORITY : Int={
    return 0
  }
  def getQUANTITY :Int={
    var valeur=new Methodes
    return valeur.getValeurAleatoire(1, 50)
  }
  def getEXTENDEDPRICE(quantity :Int ,partkey :Int) :Int ={
    var extendedprice,retailprice=0;
    retailprice = (90000 + ((partkey/10)% 20001 ) + 100 * (partkey %1000))/100;
    extendedprice = quantity * retailprice;
    return extendedprice
  }
  def getORDERTOTALPRICE(extendedprice : Int,discount : Int,tax:Int):Int={
       return (extendedprice*(1+tax)*(1-discount)*(-1))
  }
  def getDISCOUNT : Int ={
    var valeur=new Methodes
    return valeur.getValeurAleatoire(0, 10)
  }
  def getREVENUE(extendedprice : Int,discount : Int):Int={
    return (extendedprice*(100-discount))/100
  }
  def getSUPPLYCOST : Int ={
    var valeur=new Methodes
    return valeur.getValeurAleatoire(100, 100000)
  }
  def getTAX : Int ={
    var valeur=new Methodes
    return valeur.getValeurAleatoire(0, 8)
  }
  def getCOMMITDATE(orderdate : String) : String ={
    var cal=util.Calendar.getInstance()
    var date1=new SimpleDateFormat("yyyyMMdd").parse(orderdate);
    cal.setTime(date1)
    var valeur=new Methodes
    cal.add(util.Calendar.DATE, valeur.getValeurAleatoire(30,90))
    return ""+cal.get(util.Calendar.YEAR)+(cal.get(util.Calendar.MONTH))+cal.get(util.Calendar.DAY_OF_MONTH)
  }
  def getSHIPMODE:String={
    val mode = Array("REG AIR","AIR","RAIL","SHIP","TRUCK","MAIL","FOB" )
    var valeur=new Methodes
    return mode(valeur.getValeurAleatoire(0, 7))
  }

}

