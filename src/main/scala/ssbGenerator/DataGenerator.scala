package ssbGenerator

import java.io.{File, FileWriter}
import java.lang.Math.floor
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSONObject

class DataGenerator {

  def map(scaleFactor:Double,nbrmap:Int,formatSortie:String,mode:String) = {

    var fichierSortie = formatSortie
    var nbrmaptotal =nbrmap.toDouble
    var methode=2 /* all in map=1 , dimension in map and fact on reduce=2 */
    val conf = new SparkConf().setAppName("DBGen")
    if(mode=="Local"){

        conf.setMaster("local[*]")
       .set("spark.driver.host", "localhost")
    }else{

       conf.setMaster("spark://192.168.240.1:7077")
       //.setjar
      //.setOtherThings
    }


    val sc =
      SparkContext
        .getOrCreate(conf)
    var m = new Methodes
    if(methode==1) {
    var taillecust = 30000 * scaleFactor
    var taillesupp = 2000 * scaleFactor
    var taillepart = 200000 * scaleFactor
    var tailledate = 365 * 7
    var taillelineord = scaleFactor * 6000000
    var smtaille = taillesupp + taillepart + taillecust + tailledate+taillelineord

    var maps = (taillesupp.toDouble / smtaille.toDouble) * nbrmaptotal
    var mapc =(taillecust.toDouble  / smtaille.toDouble ) * nbrmaptotal
    var mapp = (taillepart.toDouble / smtaille.toDouble)* nbrmaptotal
    var mapd = (tailledate.toDouble  / smtaille.toDouble ) * nbrmaptotal
    var mapf =(tailledate.toDouble /smtaille.toDouble) * nbrmaptotal

  var rddsupp2 = sc.parallelize(List(1 to maps.toInt)).collect()
  var rddsupplier = rddsupp2.map { x => mapsupp(scaleFactor, fichierSortie) }


  val rddcust = sc.parallelize(List(1 to mapc.toInt)).collect()
  var rddcustomer = rddcust.map { x => mapcustomer(scaleFactor, fichierSortie) }


  val rddpar = sc.parallelize(List(1 to mapp.toInt)).collect()
  var rddpart = rddpar.map { x => mappart(scaleFactor, fichierSortie) }

  val rdddatee = sc.parallelize(List(1 to mapd.toInt)).collect()
  var rdddate = rdddatee.map { x => mapdate(fichierSortie) }

  val rddfact=sc.parallelize(List(1 to mapf.toInt)).collect()
  var rddlineord=rddfact.map(x=>f(scaleFactor,fichierSortie))

}
   else{
      var taillecust = 30000 * scaleFactor
      var taillesupp = 2000 * scaleFactor
      var taillepart = 200000 * scaleFactor
      var tailledate = 365 * 7
      var smtaille = taillesupp + taillepart + taillecust + tailledate

      var maps = (taillesupp.toDouble / smtaille.toDouble) * nbrmaptotal
      var mapc =(taillecust.toDouble  / smtaille.toDouble ) * nbrmaptotal
      var mapp = (taillepart.toDouble / smtaille.toDouble)* nbrmaptotal
      var mapd = (tailledate.toDouble  / smtaille.toDouble ) * nbrmaptotal

      if(maps.toInt==0){
        maps=1
        mapp=mapp-1
      }
      if(mapc.toInt==0){
        mapc=1
        mapp=mapp-1
      }
      if(mapd.toInt==0){
        mapd=1
        mapp=mapp-1
      }

      var rddsupp2 = sc.parallelize(List(1 to maps.toInt)).collect()
      var rddsupplier = rddsupp2.map { x => mapsupp(scaleFactor, fichierSortie) }


      val rddcust = sc.parallelize(List(1 to mapc.toInt)).collect()
      var rddcustomer = rddcust.map { x => mapcustomer(scaleFactor, fichierSortie) }


      val rddpar = sc.parallelize(List(1 to mapp.toInt)).collect()
      var rddpart = rddpar.map { x => mappart(scaleFactor, fichierSortie) }

      val rdddatee = sc.parallelize(List(1 to mapd.toInt)).collect()
      var rdddate = rdddatee.map { x => mapdate(fichierSortie) }

      val rddfact = ((rddsupplier.union(rddcustomer)).union(rddpart)).union(rdddate)
      var rddlineord= rddfact.reduce((x, y) => f(scaleFactor, fichierSortie))

}
    println("+++++++++++++++++++++++++++++++++++++++++++++++")

    sc.stop()
  }

    //--------------------------------------------Supplier-----------------------------
    def mapsupp(scaleFactor:Double,fichierSortie:String) = {

    var taillesupp = (2000 * scaleFactor).toInt
    var sup = new Supplier
    var supkey = 0
    var m = new Methodes

    //---------------------Creation--------------------
    var chemin = "E:\\PFE\\ssb\\scala\\Supplier" + m.getExtension (fichierSortie)
    var separateur = m.getSeparateur (fichierSortie)
    val fileSupplier = new File (chemin)
    var fileSupplierWriter = new FileWriter (fileSupplier)
    //-------------------traitement----------------------------
    if (fichierSortie == "json") {
    for (i <- 1 to taillesupp) {
    var xsup = sup.getNation
    var suppkey = sup.getSuppKey (supkey)


    var supplierjson = JSONObject.apply (Map (
    "S_SUPPKEY" -> suppkey,
    "S_NAME" -> sup.getNameKey (suppkey, "Supplier"),
    "S_ADDRESS" -> sup.getAddress,
    "S_CITY" -> sup.getCity (xsup),
    "S_NATION" -> xsup,
    "S_REGION" -> sup.getRegion (xsup),
    "S_PHONE" -> sup.getPhone (xsup)
    ) )

    fileSupplierWriter.write (supplierjson.toString () )
    fileSupplierWriter.flush ()
    fileSupplierWriter.write (System.getProperty ("line.separator") )
    supkey = suppkey

  }
  } else {
    fileSupplierWriter.write ("S_SUPPKEY" + separateur + "S_NAME" + separateur + "S_ADDRESS" + separateur +
      "S_CITY" + separateur + "S_NATION" + separateur + "S_REGION" + separateur + "S_PHONE")
    fileSupplierWriter.write (System.getProperty ("line.separator") )
    for (i <- 1 to taillesupp) {
    var xsup = sup.getNation
    var suppkey = sup.getSuppKey (supkey)
    fileSupplierWriter.write (suppkey + "" + separateur + sup.getNameKey (suppkey, "Supplier") + separateur + sup.getAddress + separateur
    + sup.getCity (xsup) + separateur + xsup + separateur + sup.getRegion (xsup) + separateur + sup.getPhone (xsup) )

    fileSupplierWriter.write (System.getProperty ("line.separator") )
    supkey = suppkey
  }

  }
    fileSupplierWriter.close ()
  }

    //2--------------------------------------part---------------------------------
    def mappart(scaleFactor:Double,fichierSortie:String) = {
    var m = new Methodes
    var taillepart = (200000 * floor (1 + m.log2 (scaleFactor) ) ).toInt
    //---------------------Creation--------------------
    var chemin = "E:\\PFE\\ssb\\scala\\Part" + m.getExtension (fichierSortie)
    var separateur = m.getSeparateur (fichierSortie)
    val filePart = new File (chemin)
    var filePartWriter = new FileWriter (filePart)
    //-------------------traitement----------------------------


    var part = new Part
    var ptkey = 0
    if (fichierSortie == "json") {
    for (i <- 1 to taillepart) {

    var partkey = part.getPartKey (ptkey)
    var valeur1 = m.getValeurAleatoire (1, 5)
    var valeur2 = m.getValeurAleatoire (1, 5)

      var partjson = JSONObject.apply (Map (
    "P_PARTKEY" -> partkey,
    "P_NAME" -> part.getName,
    "P_MFGR" -> part.getMFGR (valeur1),
    "P_CATEGORY" -> part.getCategory (valeur1, valeur2),
    "P_BRAND1" -> part.getBRAND1 (valeur1, valeur2),
    "P_COLOR" -> part.getColor,
    "P_TYPE" -> part.getType,
    "P_SIZE" -> part.getSize,
    "P_CONTAINER" -> part.getContainer
    ) )

    filePartWriter.write (partjson.toString () )
    filePartWriter.flush ()
    filePartWriter.write (System.getProperty ("line.separator") )

    ptkey = partkey
  }
    //---------------

  } else {
    filePartWriter.write ("P_PARTKEY" + separateur + "P_NAME" + separateur + "P_MFGR" + separateur +
      "P_CATEGORY" + separateur + "P_BRAND1" + separateur + "P_COLOR" + separateur
    + "P_TYPE" + separateur + "P_SIZE" + separateur + "P_CONTAINER")
    filePartWriter.write (System.getProperty ("line.separator") )
    for (i <- 1 to taillepart) {
    var partkey = part.getPartKey (ptkey)
    var valeur1 = m.getValeurAleatoire (1, 5)
    var valeur2 = m.getValeurAleatoire (1, 5)

    filePartWriter.write ("" + partkey + separateur + part.getName + separateur + part.getMFGR (valeur1) +
    separateur + part.getCategory (valeur1, valeur2) + separateur + part.getBRAND1 (valeur1, valeur2) + separateur +
    part.getColor + separateur + part.getType + separateur + part.getSize + separateur + part.getContainer)
    filePartWriter.write (System.getProperty ("line.separator") )

    ptkey = partkey
  }

  }

    filePartWriter.close ()
  }


    //3---------------------------------------customer---------------------------------
    def mapcustomer(scaleFactor:Double,fichierSortie:String) = {
    var taillecust = (30000 * scaleFactor).toInt
    var m = new Methodes
    //------------------creation----------------------------
    var chemin = "E:\\PFE\\ssb\\scala\\Customer" + m.getExtension (fichierSortie)
    var separateur = m.getSeparateur (fichierSortie)
    val fileCustomer = new File (chemin)
    var fileCustomerWriter = new FileWriter (fileCustomer)
    //-------------------traitement----------------------------
    var cust = new Customer
    var supp = new Supplier
    var ckey = 0
    if (fichierSortie == "json") {
    for (i <- 1 to taillecust) {
    var nation = supp.getNation
    var custkey = cust.getCustKey (ckey)

      var customerjson = JSONObject.apply (Map (
    "C_CUSTKEY" -> custkey,
    "C_NAME" -> supp.getNameKey (custkey, "Customer"),
    "C_ADDRESS" -> supp.getAddress,
    "C_CITY" -> supp.getCity (nation),
    "C_NATION" -> nation,
    "C_REGION" -> supp.getRegion (nation),
    "C_PHONE" -> supp.getPhone (nation),
    "C_MKTSEGMENT" -> cust.getMKTSEGMENT
    ) )

    fileCustomerWriter.write (customerjson.toString () )
    fileCustomerWriter.flush ()
    fileCustomerWriter.write (System.getProperty ("line.separator") )
    ckey = custkey

  }
  } else {
    fileCustomerWriter.write ("C_CUSTKEY" + separateur + "C_NAME" + separateur + "C_ADDRESS" + separateur +
      "C_CITY" + separateur + "C_NATION" + separateur + "C_REGION" + separateur + "C_PHONE" + separateur + "C_MKTSEGMENT")
    fileCustomerWriter.write (System.getProperty ("line.separator") )
    for (i <- 1 to taillecust) {
    var nation = supp.getNation
    var custkey = cust.getCustKey (ckey)
    fileCustomerWriter.write ("" + custkey + separateur + supp.getNameKey (custkey, "Customer") + separateur +
    supp.getAddress + separateur + supp.getCity (nation) + nation + separateur + supp.getRegion (nation) + separateur + supp.getPhone (nation) + separateur + cust.getMKTSEGMENT)
    fileCustomerWriter.write (System.getProperty ("line.separator") )
    ckey = custkey
  }


  }
    fileCustomerWriter.close ()
  }

    //--------------------------------------Date---------------------------------
    def mapdate(fichierSortie:String) = {
    var m = new Methodes
    //------------------creation----------------------------
    var chemin = "E:\\PFE\\ssb\\scala\\Date" + m.getExtension (fichierSortie)
    var separateur = m.getSeparateur (fichierSortie)
    val fileDate = new File (chemin)
    var fileDateWriter = new FileWriter (fileDate)
    //-------------------traitement----------------------------

    var cal = Calendar.getInstance ()
    var cal2 = Calendar.getInstance ()
    cal2.set (2012, 1, 1)
    val format = new SimpleDateFormat ("yyyy-MM-dd")
    var formatted1 = format.format (cal.getTime () )
    var formatted2 = format.format (cal2.getTime () )
    var date = new Date

      if (fichierSortie == "json") {

    while (formatted1.compareTo (formatted2) >= 0) {
    var datejson = JSONObject.apply (Map (
    "D_DATEKEY" -> date.getDATEKEY (cal),
    "D_DATE" -> date.getDATE (cal),
    "D_DAYOFWEEK" -> date.getDAYOFWEEK (cal),
    "D_MONTH" -> date.getMONTH (cal),
    "D_YEAR" -> date.getYEAR (cal),
    "D_YEARMONTHNUM" -> date.getYEARMONTHNUM (cal),
    "D_YEARMONTH" -> date.getYEARMONTH (cal),
    "D_DAYNUMINWEEK" -> date.getDAYNUMINWEEK (cal),
    "D_DAYNUMINMONTH" -> date.getDAYNUMINMONTH (cal),
    "D_DAYNUMINYEAR" -> date.getDAYNUMINYEAR (cal),
    "D_MONTHNUMINYEAR" -> date.getMONTHNUMINYEAR (cal),
    "D_SELLINGSEASON" -> date.getSELLINGSEASON (cal),
    "D_LASTDAYINWEEKFL" -> date.getLastDayInWeekFL (cal),
    "D_LASTDAYINMONTHFL" -> date.getLastDayInMonthFL (cal),
    "D_HOLIDAYFL" -> date.getHOLIDAYFL (cal),
    "D_WEEKDAYFL" -> date.getWEEKDAYFL (cal)
    ) )
    fileDateWriter.write (datejson.toString () )
    fileDateWriter.flush ()
    fileDateWriter.write (System.getProperty ("line.separator") )

    cal.add (Calendar.DATE, - 1)
    formatted1 = format.format (cal.getTime () )

  }
  }
    else {
    fileDateWriter.write ("D_DATEKEY" + separateur + "D_DATE" + separateur + "D_DAYOFWEEK" + separateur + "D_MONTH" +
      separateur + "D_YEAR" + separateur + "D_YEARMONTHNUM" + separateur + "D_YEARMONTH" + separateur + "D_DAYNUMINWEEK" +
      separateur + "D_DAYNUMINMONTH" + separateur + "D_DAYNUMINYEAR" + separateur + "D_MONTHNUMINYEAR" + separateur + "D_SELLINGSEASON" +
      separateur + "D_LASTDAYINWEEKFL" + separateur + "D_LASTDAYINMONTHFL" + separateur + "D_HOLIDAYFL" + separateur + "D_WEEKDAYFL")
    fileDateWriter.write (System.getProperty ("line.separator") )

    while (formatted1.compareTo (formatted2) >= 0) {

    fileDateWriter.write (date.getDATEKEY (cal) + separateur + date.getDATE (cal) + separateur + date.getDAYOFWEEK (cal) + separateur + date.getMONTH (cal)
    + separateur + date.getYEAR (cal) + separateur + date.getYEARMONTHNUM (cal) + separateur + date.getYEARMONTH (cal)
    + separateur + date.getDAYNUMINWEEK (cal) + separateur + date.getDAYNUMINMONTH (cal) + separateur + date.getDAYNUMINYEAR (cal)
    + separateur + date.getMONTHNUMINYEAR (cal) + separateur + date.getSELLINGSEASON (cal) + separateur + date.getLastDayInWeekFL (cal)
    + separateur + date.getLastDayInMonthFL (cal) + separateur + date.getHOLIDAYFL (cal) + separateur + date.getWEEKDAYFL (cal) )
    fileDateWriter.write (System.getProperty ("line.separator") )


    cal.add (Calendar.DATE, - 1);
    formatted1 = format.format (cal.getTime () );

  }
  }
    fileDateWriter.close ()

  }


    def f(scaleFactor:Double,fichierSortie:String) = {

    //--------------------------------------LineOrder---------------------------------
    var m = new Methodes
    //------------------creation----------------------------
    var chemin = "E:\\PFE\\ssb\\scala\\LineOrder" + m.getExtension (fichierSortie)
    var separateur = m.getSeparateur (fichierSortie)
    val fileLineOrder = new File (chemin)
    var fileLineOrderWriter = new FileWriter (fileLineOrder)
    //-------------------traitement----------------------------
    var taillelineord = scaleFactor * 6000000
    var nbrlign = 1
    var orderkey = 1
    var LineOrder = new LineOrder
    var indice = 1

    if (fichierSortie == "json") {
    while (orderkey <= taillelineord) {

    var custKey = LineOrder.getCUSTKEY (scaleFactor)
    var orderDate = LineOrder.getORDERDATE
    var orderPriority = LineOrder.getORDERPRIORITY
    var lineNumberVariable = m.getValeurAleatoire (1, 7)
    for (lineNumber <- 1 to lineNumberVariable) {

    var partkey = LineOrder.getPARTKEY (scaleFactor)
    var suppkey = LineOrder.getSUPPKEY (scaleFactor)
    var shippriority = LineOrder.getSHIPPRIORITY
    var quantity = LineOrder.getQUANTITY
    var extendedprice = LineOrder.getEXTENDEDPRICE (quantity, partkey)
    var discount = LineOrder.getDISCOUNT
    var revenue = LineOrder.getREVENUE (extendedprice, discount)
    var supplycost = LineOrder.getSUPPLYCOST
    var tax = LineOrder.getTAX
    var ordertotalprice=LineOrder.getORDERTOTALPRICE(extendedprice,discount,tax)
    var commitdate = LineOrder.getCOMMITDATE (orderDate)
    var shipmode = LineOrder.getSHIPMODE
      var lineorderjson = JSONObject.apply (Map (
    "LO_ORDERKEY" -> orderkey,
    "LO_LINENUMBER" -> 1, //,
    "LO_CUSTKEY" -> custKey,
    "LO_PARTKEY" -> partkey,
    "LO_SUPPKEY" -> suppkey,
    "LO_ORDERDATE" -> orderDate,
    "LO_ORDERPRIORITY" -> orderPriority,
    "LO_SHIPPRIORITY" -> shippriority,
    "LO_QUANTITY" -> quantity,
    "LO_EXTENDEDPRICE" -> extendedprice,
    "LO_ORDTOTALPRICE" ->ordertotalprice,
    "LO_DISCOUNT" -> discount,
    "LO_REVENUE" -> revenue,
    "LO_SUPPLYCOST" -> supplycost,
    "LO_TAX" -> tax,
    "LO_COMMITDATE" -> commitdate,
    "LO_SHIPMODE" -> shipmode
    ) )

    fileLineOrderWriter.write (lineorderjson.toString () )
    fileLineOrderWriter.flush ()
    fileLineOrderWriter.write (System.getProperty ("line.separator") )

  }
    if (indice >= 7) {
    orderkey = orderkey + 25
    indice = 0
  } else {
    orderkey = orderkey + 1
    indice = indice + 1
  }
  }
  } else {

      fileLineOrderWriter.write ("LO_ORDERKEY" + separateur + "LO_LINENUMBER" + separateur + "LO_CUSTKEY" + separateur + "LO_PARTKEY" + separateur + "LO_SUPPKEY" + separateur +
        "LO_ORDERDATE" + separateur + "LO_ORDERPRIORITY" + separateur + "LO_SHIPPRIORITY"
         + "LO_QUANTITY" + separateur + "LO_EXTENDEDPRICE" + separateur + "LO_ORDTOTALPRICE" + separateur + "LO_DISCOUNT" + separateur +
        "LO_REVENUE" + separateur + "LO_SUPPLYCOST" + separateur + "LO_TAX" + separateur + "LO_COMMITDATE" + separateur + "LO_SHIPMODE")
    fileLineOrderWriter.write (System.getProperty ("line.separator") )

    while (orderkey <= taillelineord){

    var custKey = LineOrder.getCUSTKEY (scaleFactor)
    var orderDate = LineOrder.getORDERDATE
    var orderPriority = LineOrder.getORDERPRIORITY
    for (lineNumber <- 1 to 2) {

    var partkey = LineOrder.getPARTKEY (scaleFactor)
    var suppkey = LineOrder.getSUPPKEY (scaleFactor)
    var shippriority = LineOrder.getSHIPPRIORITY
    var quantity = LineOrder.getQUANTITY
    var extendedprice = LineOrder.getEXTENDEDPRICE (quantity, partkey)
    var discount = LineOrder.getDISCOUNT
    var revenue = LineOrder.getREVENUE (extendedprice, discount)
    var supplycost = LineOrder.getSUPPLYCOST
    var tax = LineOrder.getTAX
    var ordertotalprice=LineOrder.getORDERTOTALPRICE(extendedprice,discount,tax)
    var commitdate = LineOrder.getCOMMITDATE (orderDate)
    var shipmode = LineOrder.getSHIPMODE

    fileLineOrderWriter.write (orderkey + separateur + lineNumber + separateur + custKey + separateur + partkey +
    separateur + suppkey + separateur + orderDate + separateur + orderPriority + separateur + shippriority + separateur +
    quantity + separateur + extendedprice + separateur +ordertotalprice+separateur+ discount + separateur + revenue + separateur + supplycost + separateur +
    tax + separateur + commitdate + separateur + shipmode)

    fileLineOrderWriter.write (System.getProperty ("line.separator") )

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
    fileLineOrderWriter.close ()

  }

}
