package ssbGenerator

import java.lang.Math.floor
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.mongodb.scala.{Document, MongoClient, MongoDatabase}

import scala.util.parsing.json.JSONObject

class DBGenLoad {
  def GenLoad(scaleFactor: Double, nbrmap: Int) = {

    var ScaleFactor = scaleFactor
    var nbrmaptotal = nbrmap.toInt

    val conf = new SparkConf()
      .setAppName("ssb")
      .setMaster("local[*]")
      .set("spark.driver.host", "localhost")

    val sc =
      SparkContext
        .getOrCreate(conf)
    //-------------------
    val uri: String = "mongodb://localhost:27017/test?gssapiServiceName=mongodb"
    System.setProperty("org.mongodb.async.type", "netty")
    val client: MongoClient = MongoClient(uri)
    val db: MongoDatabase = client.getDatabase("test")
    //---------------------------Supplier----------------
    db.createCollection("Supplier")
    val colSupplier = db.getCollection("Supplier")
    //------------------Part------------------------
    db.createCollection("Part")
    val colPart = db.getCollection("Part")
    //----------------Customer-------------
    db.createCollection("Customer")
    val colCustomer = db.getCollection("Customer")
    //------------------Date-------------------
    db.createCollection("Date")
    val colDate = db.getCollection("Date")
    //------------------LineOrder---------------------
    db.createCollection("LineOrder")
    val colLineOrder = db.getCollection("LineOrder")
    var m = new Methodes
    var taillecust = 30000 * ScaleFactor
    var taillesupp = 2000 * ScaleFactor
    var taillelineord = ScaleFactor * 6000000
    var taillepart = (200000 * floor(1 + m.log2(ScaleFactor))).toInt
    var smtaille = taillesupp + taillepart + taillecust + taillelineord + 3000


    var nbrmapdim = nbrmaptotal / 2
    var nbrmapfait = nbrmaptotal / 2
    var coefsupp = Math.round(taillesupp / smtaille) * nbrmapdim
    var coefcust = Math.round(taillecust / smtaille) * nbrmapdim
    var coefpart = Math.round(taillepart / smtaille) * nbrmapdim
    var coefdate = Math.round(3000 / smtaille) * nbrmapdim
    var coeflinord = 8 //(Math.round(taillelineord/smtaille)*nbrmaptotal)-2


    var rddsupp2 = sc.parallelize(List(1 to coefsupp.toInt)).collect()
    var rddsupplier = rddsupp2.map { x => mapsupp(ScaleFactor) }


    val rddcust = sc.parallelize(List(1 to coefcust.toInt)).collect()
    var rddcustomer = rddcust.map { x => mapcustomer(ScaleFactor) }


    val rddpar = sc.parallelize(List(1 to coefpart.toInt)).collect()
    var rddpart = rddpar.map { x => mappart(ScaleFactor) }

    val rdddatee = sc.parallelize(List(1 to coefdate.toInt)).collect()
    var rdddate = rdddatee.map { x => mapdate(ScaleFactor) }


    val rddt = (rddsupplier.union(rddcustomer))
    var rddlineorder2 = rddt.reduce((x, y) => f(ScaleFactor))




    sc.stop()
  }

    def mapsupp(ScaleFactor: Double) = {


      //2---------------------------------------supplier---------------------------------
      //-------------------
      val uri: String = "mongodb://localhost:27017/test?gssapiServiceName=mongodb"
      System.setProperty("org.mongodb.async.type", "netty")
      val client: MongoClient = MongoClient(uri)
      val db: MongoDatabase = client.getDatabase("test")
      //---------------------------Supplier----------------
      db.createCollection("Supplier")
      val colSupplier = db.getCollection("Supplier")
      //------------------
      var taillesupp = (2000 * ScaleFactor).toInt


      var sup = new Supplier
      var supkey = 0
      for (i <- 1 to taillesupp) {
        var xsup = sup.getNation
        var suppkey = sup.getSuppKey(supkey)


        var supplierjson = JSONObject.apply(Map(
          "SuppID" -> suppkey,
          "SuppName" -> sup.getNameKey(suppkey, "Supplier"),
          "Address" -> sup.getAddress,
          "City" -> sup.getCity(xsup),
          "Nation" -> xsup,
          "Region" -> sup.getRegion(xsup),
          "Phone" -> sup.getPhone(xsup)
        ))

        try {
          colSupplier.insertOne(Document(supplierjson.toString())).head()
        }
        catch {
          case ex: Exception => println("Erreur, exception dans Supplier.")
        }
        supkey = suppkey

      }


    }

    //2--------------------------------------part---------------------------------
    def mappart(ScaleFactor: Double) = {
      //-------------------
      val uri: String = "mongodb://localhost:27017/test?gssapiServiceName=mongodb"
      System.setProperty("org.mongodb.async.type", "netty")
      val client: MongoClient = MongoClient(uri)
      val db: MongoDatabase = client.getDatabase("test")

      //------------------Part------------------------
      db.createCollection("Part")
      val colPart = db.getCollection("Part")
      //----------------
      var m = new Methodes
      var taillepart = (200000 * floor(1 + m.log2(ScaleFactor))).toInt


      var part = new Part
      var ptkey = 0
      for (i <- 1 to taillepart) {

        var partkey = part.getPartKey(ptkey)
        var valeur1 = m.getValeurAleatoire(1, 5)
        var valeur2 = m.getValeurAleatoire(1, 5)

        var partjson = JSONObject.apply(Map(
          "PARTKEY" -> partkey,
          "NAME" -> part.getName,
          "MFGR" -> part.getMFGR(valeur1),
          "CATEGORY" -> part.getCategory(valeur1, valeur2),
          "BRAND1" -> part.getBRAND1(valeur1, valeur2),
          "COLOR" -> part.getColor,
          "TYPE" -> part.getType,
          "SIZE" -> part.getSize,
          "CONTAINER" -> part.getContainer
        ))

        try {

          colPart.insertOne(Document(partjson.toString())).head()
        } catch {
          case ex: Exception => println("Erreur, exception dans Part.")
        }

        ptkey = partkey

        //---------------

      }

    }


    //3---------------------------------------customer---------------------------------
    def mapcustomer(ScaleFactor: Double) = {
      //-------------------
      val uri: String = "mongodb://localhost:27017/test?gssapiServiceName=mongodb"
      System.setProperty("org.mongodb.async.type", "netty")
      val client: MongoClient = MongoClient(uri)
      val db: MongoDatabase = client.getDatabase("test")

      //----------------Customer-------------
      db.createCollection("Customer")
      val colCustomer = db.getCollection("Customer")
      //------------------
      var taillecust = (30000 * ScaleFactor).toInt


      var cust = new Customer
      var supp = new Supplier
      var ckey = 0
      for (i <- 1 to taillecust) {
        var nation = supp.getNation
        var custkey = cust.getCustKey(ckey)

        var customerjson = JSONObject.apply(Map(
          "CustID" -> custkey,
          "CustName" -> supp.getNameKey(custkey, "Customer"),
          "Address" -> supp.getAddress,
          "City" -> supp.getCity(nation),
          "Nation" -> nation,
          "Region" -> supp.getRegion(nation),
          "Phone" -> supp.getPhone(nation),
          "MkTsegment" -> cust.getMKTSEGMENT
        ))

        try {
          colCustomer.insertOne(Document(customerjson.toString())).head()
        }
        catch {
          case ex: Exception => println("Erreur, exception dans Customer.")
        }
        ckey = custkey

      }

    }

    //--------------------------------------Date---------------------------------
    def mapdate(ScaleFactor: Double) = {
      //-------------------
      val uri: String = "mongodb://localhost:27017/test?gssapiServiceName=mongodb"
      System.setProperty("org.mongodb.async.type", "netty")
      val client: MongoClient = MongoClient(uri)
      val db: MongoDatabase = client.getDatabase("test")

      //------------------Date-------------------
      db.createCollection("Date")
      val colDate = db.getCollection("Date")
      //------------------


      var cal = Calendar.getInstance()
      var cal2 = Calendar.getInstance()
      cal2.set(2012, 1, 1)
      val format = new SimpleDateFormat("yyyy-MM-dd")
      var formatted1 = format.format(cal.getTime())
      var formatted2 = format.format(cal2.getTime())
      var date = new Date
      var i = 0
      while (formatted1.compareTo(formatted2) >= 0) {
        var datejson = JSONObject.apply(Map(
          "DateKey" -> date.getDATEKEY(cal),
          "Date" -> date.getDATE(cal),
          "DayOfWeek" -> date.getDAYOFWEEK(cal),
          "Month" -> date.getMONTH(cal),
          "Year" -> date.getYEAR(cal),
          "YearMonthNum" -> date.getYEARMONTHNUM(cal),
          "YearMonth" -> date.getYEARMONTH(cal),
          "DayNumInWeek" -> date.getDAYNUMINWEEK(cal),
          "DayNumInMonth" -> date.getDAYNUMINMONTH(cal),
          "DayNumInYear" -> date.getDAYNUMINYEAR(cal),
          "MonthNumYear" -> date.getMONTHNUMINYEAR(cal),
          "SellingSeason" -> date.getSELLINGSEASON(cal),
          "LastDayInWeekFL" -> date.getLastDayInWeekFL(cal),
          "LastDayInMonthFL" -> date.getLastDayInMonthFL(cal),
          "HolidayFL" -> date.getHOLIDAYFL(cal),
          "WeekDayFL" -> date.getWEEKDAYFL(cal)
        ))
        try {
          colDate.insertOne(Document(datejson.toString())).head()
        }
        catch {
          case ex: Exception => println("Erreur, exception dans Date.")
        }

        i = i + 1
        cal.add(Calendar.DATE, -1)
        formatted1 = format.format(cal.getTime())

      }


    }


    def f(ScaleFactor: Double) = {

      //--------------------------------------LineOrder---------------------------------
      //6000000
      //-------------------
      val uri: String = "mongodb://localhost:27017/test?gssapiServiceName=mongodb"
      System.setProperty("org.mongodb.async.type", "netty")
      val client: MongoClient = MongoClient(uri)
      val db: MongoDatabase = client.getDatabase("test")

      //------------------LineOrder---------------------
      db.createCollection("LineOrder")
      val colLineOrder = db.getCollection("LineOrder")


      var nbrlign = 1;
      var orderkey = 1;
      var LineOrder = new LineOrder
      var indice = 1
      while (orderkey <= 6000000 * ScaleFactor) {

        var custKey = LineOrder.getCUSTKEY(ScaleFactor)
        var orderDate = LineOrder.getORDERDATE
        var orderPriority = LineOrder.getORDERPRIORITY
        for (lineNumber <- 1 to 2) {

          var partkey = LineOrder.getPARTKEY(ScaleFactor)
          var suppkey = LineOrder.getSUPPKEY(ScaleFactor)
          var shippriority = LineOrder.getSHIPPRIORITY
          var quantity = LineOrder.getQUANTITY
          var extendedprice = LineOrder.getEXTENDEDPRICE(quantity, partkey)
          var discount = LineOrder.getDISCOUNT
          var revenue = LineOrder.getREVENUE(extendedprice, discount)
          var supplycost = LineOrder.getSUPPLYCOST
          var tax = LineOrder.getTAX
          var commitdate = LineOrder.getCOMMITDATE(orderDate)
          var shipmode = LineOrder.getSHIPMODE

          var lineorderjson = JSONObject.apply(Map(
            "ORDERKEY" -> orderkey,
            "LINENUMBER" -> lineNumber,
            "CUSTKEY" -> custKey,
            "PARTKEY" -> partkey,
            "SUPPKEY" -> suppkey,
            "ORDERDATE" -> orderDate,
            "ORDPRIORITY" -> orderPriority,
            "SHIPPRIORITY" -> shippriority,
            "QUANTITY" -> quantity,
            "EXTENDEDPRICE" -> extendedprice,
            "DISCOUNT" -> discount,
            "REVENUE" -> revenue,
            "SUPPLYCOST" -> supplycost,
            "TAX" -> tax,
            "COMMITDATE" -> commitdate,
            "SHIPMODE" -> shipmode
          ))

          try {
            colLineOrder.insertOne(Document(lineorderjson.toString())).head()
          }
          catch {
            case ex: Exception => println("Erreur, exception dans LineOrder.")
          }

        }
        if (indice >= 7) {
          orderkey = orderkey + 25
          indice = 0
        } else {
          orderkey = orderkey + 1
          indice = indice + 1
        }
      }


    }



}