package ssbGenerator

import java.awt.{Color, EventQueue, Font, SystemColor}
import java.awt.event.{ActionEvent, ActionListener, ItemEvent, ItemListener}

import javax.swing._
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.File
import java.io.FileWriter

import scala.util.parsing.json.JSONObject
import java.lang.Math.floor

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession
import org.mongodb.scala._

import scala.io.Source
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.sql._
import org.apache.spark.sql.SparkSession


object GenerateurSSB {

  /**
    * Launch the application.
    */
  def main(args: Array[String]): Unit = {
    EventQueue.invokeLater(new Runnable() {
      def run(): Unit = {
        try {
          val window: GenerateurSSB = new GenerateurSSB()
          window.frame.setVisible(true)
        } catch {
          case e: Exception => e.printStackTrace()

        }
      }
    })
  }

}

class GenerateurSSB {

  private var frame: JFrame = _

  private var sf: JTextField = _

  private var nbrMap: JTextField = _

  private var regionCustomerQ3: JTextField = _

  private var regionSupplierQ3: JTextField = _

  private var yearDebut: JTextField = _

  private var yearFin: JTextField = _

  private var textField: JTextField = _

  private var textField_1: JTextField = _

  private var textField_2: JTextField = _

  private var textField_3: JTextField = _

  private var yearQ1: JTextField = _

  private var textField_4: JTextField = _

  private var discountValMax: JTextField = _

  private var quantity: JTextField = _

  private var categoryQ2: JTextField = _

  private var regionSupplierQ2: JTextField = _




  initialize()

  /**
    * Initialize the contents of the frame.
    */

  private def initialize(): Unit = {
    frame = new JFrame("OGGB NOSQL")
    frame.setBounds(100, 100, 750, 600)
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
    frame.getContentPane.setLayout(null)
    val panel: JPanel = new JPanel()
    panel.setBackground(new Color(255, 255, 255))
    panel.setBounds(0, 0, 733, 561)
    frame.getContentPane.add(panel)
    panel.setLayout(null)
    val tabbedPane: JTabbedPane = new JTabbedPane(JTabbedPane.SCROLL_TAB_LAYOUT) //top)
    tabbedPane.setBounds(0, 86, 733, 475)
    panel.add(tabbedPane)
    val panel_1: JPanel = new JPanel()
    panel_1.setBackground(new Color(255, 255, 255))
    tabbedPane.addTab("SSB", null, panel_1, null)
    panel_1.setLayout(null)
    val btnSupplier: JButton = new JButton("Supplier")
    btnSupplier.addActionListener(new ActionListener() {
      def actionPerformed(arg0: ActionEvent): Unit = {
        //*****************************************************************************
        val window: SupplierInterface = new SupplierInterface()
        window.frame.setVisible(true)
      }
    })
    btnSupplier.setForeground(new Color(0, 0, 0))
    btnSupplier.setFont(new Font("Tahoma", Font.BOLD | Font.ITALIC, 15))
    btnSupplier.setBackground(new Color(240, 240, 240))
    btnSupplier.setBounds(49, 21, 137, 95)
    panel_1.add(btnSupplier)
    val btnPart: JButton = new JButton("Part")
    btnPart.addActionListener(new ActionListener() {
      def actionPerformed(arg0: ActionEvent): Unit = {
        //*****************************************************************************
        val window: PartInterface = new PartInterface()
        window.frame.setVisible(true)
      }
    })
    btnPart.setBackground(SystemColor.control)
    btnPart.setFont(new Font("Tahoma", Font.BOLD | Font.ITALIC, 15))
    btnPart.setBounds(469, 21, 137, 95)
    panel_1.add(btnPart)
    val btnCustomer: JButton = new JButton("Customer")
    btnCustomer.addActionListener(new ActionListener() {
      def actionPerformed(e: ActionEvent): Unit = {
        //*****************************************************************************
        val window: CustomerInterface = new CustomerInterface()
        window.frame.setVisible(true)
      }
    })
    btnCustomer.setBackground(SystemColor.control)
    btnCustomer.setFont(new Font("Tahoma", Font.BOLD | Font.ITALIC, 15))
    btnCustomer.setBounds(49, 227, 137, 95)
    panel_1.add(btnCustomer)
    val btnDate: JButton = new JButton("Date")
    btnDate.addActionListener(new ActionListener() {
      def actionPerformed(e: ActionEvent): Unit = {
        val window: DateInterface = new DateInterface()
        window.frame.setVisible(true)
      }
    })
    btnDate.setBackground(SystemColor.control)
    btnDate.setFont(new Font("Tahoma", Font.BOLD | Font.ITALIC, 15))
    btnDate.setBounds(469, 227, 137, 95)
    panel_1.add(btnDate)
    val btnLineorder: JButton = new JButton("LineOrder")
    btnLineorder.addActionListener(new ActionListener() {
      def actionPerformed(e: ActionEvent): Unit = {
        val window: LineOrderInterface = new LineOrderInterface()
        window.frame.setVisible(true)
      }
    })

    btnLineorder.setFont(new Font("Tahoma", Font.BOLD | Font.ITALIC, 15))
    btnLineorder.setBackground(SystemColor.control)
    btnLineorder.setBounds(223, 99, 179, 142)
    panel_1.add(btnLineorder)
    val label_1: JLabel = new JLabel("")
    label_1.setIcon(
      new ImageIcon("C:\\Users\\Boulqres\\Pictures\\Camera Roll\\b.PNG"))
    label_1.setBounds(187, 39, 467, 263)
    panel_1.add(label_1)
    val panel_2: JPanel = new JPanel()
    panel_2.setBackground(Color.WHITE)
    tabbedPane.addTab("Generation and Loading", null, panel_2, null)
    panel_2.setLayout(null)
    val label: JLabel = new JLabel("Scale factor")
    label.setFont(new Font("Tahoma", Font.BOLD, 14))
    label.setBounds(207, 11, 118, 26)
    panel_2.add(label)
    sf = new JTextField()
    sf.setColumns(10)
    sf.setBounds(207, 45, 118, 34)
    panel_2.add(sf)
    val label_2: JLabel = new JLabel("Number of Map")
    label_2.setFont(new Font("Tahoma", Font.BOLD, 14))
    label_2.setBounds(384, 11, 118, 26)
    panel_2.add(label_2)
    nbrMap = new JTextField()
    nbrMap.setColumns(10)
    nbrMap.setBounds(384, 45, 118, 34)
    panel_2.add(nbrMap)
    val tconsole: JTextArea = new JTextArea()
    tconsole.setBounds(20, 349, 686, 87)
    panel_2.add(tconsole)
   /* val lblModeDeGnration: JLabel = new JLabel("")
    lblModeDeGnration.setFont(new Font("Tahoma", Font.BOLD, 14))
    lblModeDeGnration.setBounds(20, 11, 151, 26)
    panel_2.add(lblModeDeGnration)*/
    val label_3: JLabel = new JLabel("output format")
    label_3.setFont(new Font("Tahoma", Font.BOLD, 14))
    label_3.setBounds(539, 11, 118, 26)
    panel_2.add(label_3)
    val comboformatsortie = Array("csv", "tbl", "json")
    val formatdesortie = new JComboBox(comboformatsortie)
    formatdesortie.setModel(
      new DefaultComboBoxModel(Array("csv", "tbl", "json")))
    formatdesortie.setFont(new Font("Tahoma", Font.PLAIN, 16))
    formatdesortie.setBounds(539, 45, 110, 34)
    panel_2.add(formatdesortie)
    val combomodegeneration = Array("")
   // val combomodegeneration = Array("Local", "Standalone", "Cluster")
    val modegeneration = new JComboBox(combomodegeneration)
    modegeneration.setModel(
      new DefaultComboBoxModel(Array("")))
   // new DefaultComboBoxModel(Array("Local", "Standalone", "Cluster")))
    modegeneration.setFont(new Font("Tahoma", Font.PLAIN, 16))
    modegeneration.setBounds(20, 45, 138, 34)
   // panel_2.add(modegeneration)
    val label_4: JLabel = new JLabel("Generation & Loading")
    label_4.setFont(new Font("Tahoma", Font.BOLD, 14))
    label_4.setBounds(20, 128, 194, 26)
    panel_2.add(label_4)
    val generation: JRadioButton = new JRadioButton(
      "Generation then loading")
    generation.setBounds(20, 161, 194, 23)
    panel_2.add(generation)
    val generation_chargement: JRadioButton = new JRadioButton(
      "Generation & Loading")
    generation_chargement.setBounds(20, 187, 194, 23)
    panel_2.add(generation_chargement)
    val panel_3: JPanel = new JPanel()
    panel_3.setBounds(264, 100, 417, 221)
    panel_2.add(panel_3)
    panel_3.setLayout(null)
    val btnCharger: JButton = new JButton("Load")
    btnCharger.setVisible(false)
    btnCharger.addActionListener(new ActionListener() {
      def actionPerformed(arg0: ActionEvent): Unit = {
        val uri: String = "mongodb://localhost:27017/test?gssapiServiceName=mongodb"
        System.setProperty("org.mongodb.async.type", "netty")
        val client: MongoClient = MongoClient(uri)
        val db: MongoDatabase = client.getDatabase("Test")
        println("********************")
        //---------------------------
        val fileNamePart = "D:\\hadoop-2.7.1\\HDFS\\Part.json"

        db.createCollection("Part")
        val colPart = db.getCollection("Part")
        try {
          for (json <- Source.fromFile(fileNamePart).getLines()) {
            colPart.insertOne(Document(json)).head()

          }
        } catch {
          case ex: Exception => println("Error, exception in Part.")
        }
        //--------------------------------
        val fileNameDate = "D:\\hadoop-2.7.1\\HDFS\\Date.json"

        db.createCollection("Date")
        val colDate = db.getCollection("Date")
        try {
          for (json <- Source.fromFile(fileNameDate).getLines()) {
            colDate.insertOne(Document(json)).head()
          }
        } catch {
          case ex: Exception => println("Error, exception in Date.")
        }
        //--------------------------------
        val fileNameSupplier = "D:\\hadoop-2.7.1\\HDFS\\Supplier.json"

        db.createCollection("Supplier")
        val colSupplier = db.getCollection("Supplier")
        try {
          for (json <- Source.fromFile(fileNameSupplier).getLines()) {
            colSupplier.insertOne(Document(json)).head()
          }
        } catch {
          case ex: Exception => println("Error, exception in Supplier.")
        }
        //--------------------------------
        val fileNameCustomer = "D:\\hadoop-2.7.1\\HDFS\\Customer.json"

        db.createCollection("Customer")
        val colCustomer = db.getCollection("Customer")
        try {
          for (json <- Source.fromFile(fileNameCustomer).getLines()) {
            colCustomer.insertOne(Document(json)).head()
          }
        } catch {
          case ex: Exception => println("Error, exception in Customer.")
        }
        //--------------------------------
        val fileNameLineOrder = "D:\\hadoop-2.7.1\\HDFS\\LineOrder.json"

        db.createCollection("LineOrder")
        val colLineOrder = db.getCollection("LineOrder")
        try {
          for (json <- Source.fromFile(fileNameLineOrder).getLines()) {
            colLineOrder.insertOne(Document(json)).head()
          }
        } catch {
          case ex: Exception => println("Error, exception in LineOrder.")
        }
        //--------------------------------
        tconsole.setText("loading is complete")
      }

    })
    btnCharger.setFont(new Font("Tahoma", Font.BOLD | Font.ITALIC, 16))
    btnCharger.setBounds(149, 142, 144, 47)
    panel_3.add(btnCharger)
    val btngenerer: JButton = new JButton("Generate")
    btngenerer.setVisible(false)
    btngenerer.addActionListener(new ActionListener() {
      def actionPerformed(arg0: ActionEvent): Unit = {

        btnCharger.setVisible(true)
        var scaleFactor = sf.getText().toDouble
        var nbrmap = nbrMap.getText().toInt
        var formatSortie = formatdesortie.getSelectedItem.toString
        var mode = modegeneration.getSelectedItem.toString
        var generateurDonnées = new DataGenerator
        tconsole.setText("Data generation is started")
        tconsole.setText(System.in+"**")

        generateurDonnées.map(scaleFactor, nbrmap, formatSortie,"Distributed")
        tconsole.setText(System.in+"**")
        tconsole.setText("Data generation is complete ")


        //*********************************************
        sf.setText(" ")
        nbrMap.setText(" ")


      }
    })
    btngenerer.setFont(new Font("Tahoma", Font.BOLD | Font.ITALIC, 16))
    btngenerer.setBounds(149, 26, 144, 47)
    panel_3.add(btngenerer)
    val btnGnrerCharger: JButton = new JButton("Generate & Load")
    btnGnrerCharger.addActionListener(new ActionListener() {
      def actionPerformed(e: ActionEvent): Unit = {
        var scaleFactor = sf.getText().toDouble
        var nbrmap = nbrMap.getText().toInt

        var GenererCharger = new DBGenLoad
        GenererCharger.GenLoad(scaleFactor, nbrmap)
        sf.setText(" ")
        nbrMap.setText(" ")

      }
    })
    btnGnrerCharger.setVisible(false)
    btnGnrerCharger.setFont(new Font("Tahoma", Font.BOLD | Font.ITALIC, 16))
    btnGnrerCharger.setBounds(125, 84, 207, 47)
    panel_3.add(btnGnrerCharger)
    val panel_4: JPanel = new JPanel()
    panel_4.setBackground(Color.WHITE)
    tabbedPane.addTab("Interrogation", null, panel_4, null)
    panel_4.setLayout(null)
    val label_5: JLabel = new JLabel("")
    label_5.setIcon(
      new ImageIcon("C:\\Users\\Boulqres\\Pictures\\Camera Roll\\11.PNG"))
    label_5.setBounds(336, 45, 46, 323)
    panel_4.add(label_5)
    val label_6: JLabel = new JLabel("")
    label_6.setIcon(
      new ImageIcon("C:\\Users\\Boulqres\\Pictures\\Camera Roll\\222.PNG"))
    label_6.setBounds(191, 179, 513, 38)
    panel_4.add(label_6)
    val lblRequete: JLabel = new JLabel("Request1")
    lblRequete.setFont(new Font("Tahoma", Font.BOLD, 14))
    lblRequete.setBounds(24, 11, 118, 26)
    panel_4.add(lblRequete)
    val lblRequete_1: JLabel = new JLabel("Request2")
    lblRequete_1.setFont(new Font("Tahoma", Font.BOLD, 14))
    lblRequete_1.setBounds(406, 11, 118, 26)
    panel_4.add(lblRequete_1)
    val lblRequete_3: JLabel = new JLabel("Request4")
    lblRequete_3.setFont(new Font("Tahoma", Font.BOLD, 14))
    lblRequete_3.setBounds(406, 204, 118, 26)
    panel_4.add(lblRequete_3)
    val lblRequete_2: JLabel = new JLabel("Request3")
    lblRequete_2.setFont(new Font("Tahoma", Font.BOLD, 14))
    lblRequete_2.setBounds(24, 204, 118, 26)
    panel_4.add(lblRequete_2)
    val btnDtails: JButton = new JButton("Details")
    btnDtails.setFont(new Font("Tahoma", Font.BOLD, 14))
    btnDtails.setBounds(519, 409, 122, 27)
    panel_4.add(btnDtails)
    btnDtails.addActionListener(new ActionListener() {
      def actionPerformed(e: ActionEvent): Unit = {
      val window: QGenInterface = new QGenInterface()
      window.frame.setVisible(true)}

    })


    val lblSelectSumloextendedpricelodiscountAs: JLabel = new JLabel(
      "select sum(LO.EXTENDEDPRICE*LO.DISCOUNT) as revenue")
    lblSelectSumloextendedpricelodiscountAs.setBounds(10, 42, 336, 14)
    panel_4.add(lblSelectSumloextendedpricelodiscountAs)
    val lblFrom: JLabel = new JLabel("from LineOrder LO , Date D")
    lblFrom.setBounds(10, 54, 132, 14)
    panel_4.add(lblFrom)

    val lblWhere: JLabel = new JLabel("where LO.ORDERDATE =D.DateKey")
    lblWhere.setBounds(10, 67, 191, 14)
    panel_4.add(lblWhere)
    val lblNewLabel_1: JLabel = new JLabel("and D.YEAR = ")
    lblNewLabel_1.setBounds(10, 79, 82, 14)
    panel_4.add(lblNewLabel_1)
    val lblNewLabel_2: JLabel = new JLabel("and LO.DISCOUNT between ")
    lblNewLabel_2.setBounds(10, 92, 144, 14)
    panel_4.add(lblNewLabel_2)
    val lblAnd: JLabel = new JLabel("and")
    lblAnd.setBounds(204, 92, 32, 14)
    panel_4.add(lblAnd)
    val lblNewLabel_3: JLabel = new JLabel("and LO.QUANTITY < ")
    lblNewLabel_3.setBounds(10, 104, 103, 14)
    panel_4.add(lblNewLabel_3)
    val lblSelectSumlorevenueAs: JLabel = new JLabel(
      "select sum(LO.REVENUE) as revenue , D.YEAR, P.BRAND1 ")
    lblSelectSumlorevenueAs.setBounds(382, 42, 336, 14)
    panel_4.add(lblSelectSumlorevenueAs)
    val lblFromLineorderLo: JLabel = new JLabel(
      "from LineOrder LO, Date D, Part P, Supplier S")
    lblFromLineorderLo.setBounds(382, 54, 298, 14)
    panel_4.add(lblFromLineorderLo)
    val lblWhereLoorderdate: JLabel = new JLabel(
      "where LO.ORDERDATE = D.DATEKEY")
    lblWhereLoorderdate.setBounds(382, 67, 298, 14)
    panel_4.add(lblWhereLoorderdate)
    val lblAndLopartkey: JLabel = new JLabel("and LO.PARTKEY = P.PARTKEY")
    lblAndLopartkey.setBounds(382, 79, 298, 14)
    panel_4.add(lblAndLopartkey)
    val lblAndLosuppkey: JLabel = new JLabel("and LO.SUPPKEY = S.SUPPID ")
    lblAndLosuppkey.setBounds(382, 92, 298, 14)
    panel_4.add(lblAndLosuppkey)
    val lblAndPcategory: JLabel = new JLabel("and P.CATEGORY =")
    lblAndPcategory.setBounds(382, 104, 103, 14)
    panel_4.add(lblAndPcategory)
    val lblAndSregion: JLabel = new JLabel(" and S.REGION =")
    lblAndSregion.setBounds(527, 104, 89, 14)
    panel_4.add(lblAndSregion)
    val lblGroupByDyear: JLabel = new JLabel("group by D.YEAR, P.BRAND1")
    lblGroupByDyear.setBounds(382, 117, 298, 14)
    panel_4.add(lblGroupByDyear)
    val lblOrderByDyear: JLabel = new JLabel("order by D.YEAR, P.BRAND1")
    lblOrderByDyear.setBounds(382, 129, 298, 14)
    panel_4.add(lblOrderByDyear)
    val lblSelectCnationAs: JLabel = new JLabel(
      "select C.NATION as C_NATION, S.NATION AS S_NATION, D.YEAR, ")
    lblSelectCnationAs.setBounds(10, 240, 336, 14)
    panel_4.add(lblSelectCnationAs)
    val lblSumlorevenueAsRevenue: JLabel = new JLabel(
      "sum(LO.REVENUE) as revenue")
    lblSumlorevenueAsRevenue.setBounds(10, 254, 336, 14)
    panel_4.add(lblSumlorevenueAsRevenue)
    val lblFromCustomerC: JLabel = new JLabel(
      "from Customer C, LineOrder LO, Supplier S, Date D")
    lblFromCustomerC.setBounds(10, 266, 336, 14)
    panel_4.add(lblFromCustomerC)
    val lblWhereLocustkey: JLabel = new JLabel("where LO.CUSTKEY = C.CUSTID")
    lblWhereLocustkey.setBounds(10, 279, 336, 14)
    panel_4.add(lblWhereLocustkey)
    val lblAndLosuppkey_1: JLabel = new JLabel("and LO.SUPPKEY = S.SUPPID")
    lblAndLosuppkey_1.setBounds(10, 291, 336, 14)
    panel_4.add(lblAndLosuppkey_1)
    val lblAndLoorderdate: JLabel = new JLabel("and LO.ORDERDATE = D.DATEKEY")
    lblAndLoorderdate.setBounds(10, 304, 336, 14)
    panel_4.add(lblAndLoorderdate)
    val lblAndCregion: JLabel = new JLabel("and C.REGION =")
    lblAndCregion.setBounds(10, 316, 89, 14)
    panel_4.add(lblAndCregion)
    val lblAndDyear: JLabel = new JLabel(" and D.YEAR >= ")
    lblAndDyear.setBounds(10, 329, 103, 14)
    panel_4.add(lblAndDyear)

    val lblGroupByCnation: JLabel = new JLabel(
      "group by C.NATION, S.NATION, D.YEAR ")
    lblGroupByCnation.setBounds(10, 341, 336, 14)
    panel_4.add(lblGroupByCnation)
    regionCustomerQ3 = new JTextField()
    regionCustomerQ3.setColumns(10)
    regionCustomerQ3.setBounds(108, 316, 46, 14)
    panel_4.add(regionCustomerQ3)
    val lblAndSregion_1: JLabel = new JLabel("and S.REGION =")
    lblAndSregion_1.setBounds(165, 316, 89, 14)
    panel_4.add(lblAndSregion_1)
    regionSupplierQ3 = new JTextField()
    regionSupplierQ3.setColumns(10)
    regionSupplierQ3.setBounds(249, 316, 46, 14)
    panel_4.add(regionSupplierQ3)
    val lblAndDyear_1: JLabel = new JLabel(" and D.YEAR <= ")
    lblAndDyear_1.setBounds(165, 329, 91, 14)
    panel_4.add(lblAndDyear_1)
    val lblOrderByDyear_1: JLabel = new JLabel(
      "order by D.YEAR asc, REVENUE desc")
    lblOrderByDyear_1.setBounds(10, 354, 336, 14)
    panel_4.add(lblOrderByDyear_1)
    val lblSelectDyearCnation: JLabel = new JLabel(
      "select D.YEAR, C.NATION,  sum(LO.REVENUE - LO.SUPPLYCOST) as profit ")
    lblSelectDyearCnation.setBounds(371, 240, 362, 14)
    panel_4.add(lblSelectDyearCnation)
    val lblFromDateD: JLabel = new JLabel(
      "from Date D, Customer C, Supplier S, Part P, LineOrder LO")
    lblFromDateD.setBounds(371, 254, 336, 14)
    panel_4.add(lblFromDateD)
    val lblWhereLocustkey_1: JLabel = new JLabel("where LO.CUSTKEY = C.CUSTID")
    lblWhereLocustkey_1.setBounds(371, 266, 336, 14)
    panel_4.add(lblWhereLocustkey_1)
    val lblAndLosuppkey_2: JLabel = new JLabel("and LO.SUPPKEY = S.SUPPID ")
    lblAndLosuppkey_2.setBounds(371, 279, 336, 14)
    panel_4.add(lblAndLosuppkey_2)
    val lblAndLopartkey_1: JLabel = new JLabel("and LO.PARTKEY = P.PARTKEY")
    lblAndLopartkey_1.setBounds(371, 291, 336, 14)
    panel_4.add(lblAndLopartkey_1)
    yearDebut = new JTextField()
    yearDebut.setColumns(10)
    yearDebut.setBounds(108, 329, 46, 14)
    panel_4.add(yearDebut)
    yearFin = new JTextField()
    yearFin.setColumns(10)
    yearFin.setBounds(249, 329, 46, 14)
    panel_4.add(yearFin)
    val lblAndLoorderdate_1: JLabel = new JLabel(
      "and LO.ORDERDATE = D.DATEKEY ")
    lblAndLoorderdate_1.setBounds(371, 304, 336, 14)
    panel_4.add(lblAndLoorderdate_1)
    val lblAndCregion_1: JLabel = new JLabel("and C.REGION =")
    lblAndCregion_1.setBounds(371, 316, 89, 14)
    panel_4.add(lblAndCregion_1)
    val lblAndpmfgr: JLabel = new JLabel("and (P.MFGR = ")
    lblAndpmfgr.setBounds(371, 329, 82, 14)
    panel_4.add(lblAndpmfgr)
    val lblGroupByDyear_1: JLabel = new JLabel("group by D.YEAR, C.NATION ")
    lblGroupByDyear_1.setBounds(371, 341, 336, 14)
    panel_4.add(lblGroupByDyear_1)
    val lblOrderByDyear_2: JLabel = new JLabel("order by D.YEAR, C.NATION")
    lblOrderByDyear_2.setBounds(371, 354, 336, 14)
    panel_4.add(lblOrderByDyear_2)
    textField = new JTextField()
    textField.setColumns(10)
    textField.setBounds(455, 316, 46, 14)
    panel_4.add(textField)
    val lblAndSregion_2: JLabel = new JLabel("and S.REGION =")
    lblAndSregion_2.setBounds(511, 316, 89, 14)
    panel_4.add(lblAndSregion_2)
    textField_1 = new JTextField()
    textField_1.setColumns(10)
    textField_1.setBounds(595, 316, 46, 14)
    panel_4.add(textField_1)
    textField_2 = new JTextField()
    textField_2.setColumns(10)
    textField_2.setBounds(455, 329, 46, 14)
    panel_4.add(textField_2)
    val lblOrPmfgr: JLabel = new JLabel(" or P.MFGR = ")
    lblOrPmfgr.setBounds(511, 329, 89, 14)
    panel_4.add(lblOrPmfgr)
    textField_3 = new JTextField()
    textField_3.setColumns(10)
    textField_3.setBounds(595, 329, 46, 14)
    panel_4.add(textField_3)
    val label_7: JLabel = new JLabel(")")
    label_7.setBounds(651, 329, 46, 14)
    panel_4.add(label_7)
    yearQ1 = new JTextField()
    yearQ1.setColumns(10)
    yearQ1.setBounds(79, 79, 46, 14)
    panel_4.add(yearQ1)
    textField_4 = new JTextField()
    textField_4.setColumns(10)
    textField_4.setBounds(155, 92, 46, 14)
    panel_4.add(textField_4)
    discountValMax = new JTextField()
    discountValMax.setColumns(10)
    discountValMax.setBounds(233, 92, 46, 14)
    panel_4.add(discountValMax)
    quantity = new JTextField()
    quantity.setColumns(10)
    quantity.setBounds(108, 104, 46, 14)
    panel_4.add(quantity)
    categoryQ2 = new JTextField()
    categoryQ2.setColumns(10)
    categoryQ2.setBounds(478, 104, 46, 14)
    panel_4.add(categoryQ2)
    regionSupplierQ2 = new JTextField()
    regionSupplierQ2.setColumns(10)
    regionSupplierQ2.setBounds(616, 104, 46, 14)
    panel_4.add(regionSupplierQ2)
    val arraycombo = Array("json", "mongodb")
    val comboBox = new JComboBox(arraycombo)
    comboBox.setModel(
      new DefaultComboBoxModel(Array("json", "mongodb")))
    comboBox.setFont(new Font("Tahoma", Font.PLAIN, 14))
    comboBox.setBounds(79, 409, 122, 26)
    panel_4.add(comboBox)
    val lblSource: JLabel = new JLabel("Source :")
    lblSource.setFont(new Font("Tahoma", Font.BOLD, 14))
    lblSource.setBounds(18, 409, 63, 26)
    panel_4.add(lblSource)
    val btnExecuter: JButton = new JButton("Execute")
    btnExecuter.addActionListener(new ActionListener() {
      def actionPerformed(e: ActionEvent): Unit = {
        val panel_5 = new JPanel
        panel_5.setBackground(Color.WHITE)
        tabbedPane.addTab("ExecutionQGen", null, panel_5, null)
        panel_5.setLayout(null)
        val label_8 = new JLabel("Request1")
        label_8.setFont(new Font("Tahoma", Font.BOLD, 14))
        label_8.setBounds(10, 11, 118, 26)
        panel_5.add(label_8)
        val label_9 = new JLabel("Request2")
        label_9.setFont(new Font("Tahoma", Font.BOLD, 14))
        label_9.setBounds(392, 11, 118, 26)
        panel_5.add(label_9)
        val label_10 = new JLabel("")
        label_10.setIcon(new ImageIcon("C:\\Users\\Boulqres\\Pictures\\Camera Roll\\11.PNG"))
        label_10.setBounds(322, 45, 46, 323)
        panel_5.add(label_10)
        val label_11 = new JLabel("")
        label_11.setIcon(new ImageIcon("C:\\Users\\Boulqres\\Pictures\\Camera Roll\\222.PNG"))
        label_11.setBounds(177, 179, 513, 38)
        panel_5.add(label_11)
        val label_12 = new JLabel("Request4")
        label_12.setFont(new Font("Tahoma", Font.BOLD, 14))
        label_12.setBounds(392, 204, 118, 26)
        panel_5.add(label_12)
        val label_13 = new JLabel("Request3")
        label_13.setFont(new Font("Tahoma", Font.BOLD, 14))
        label_13.setBounds(10, 204, 118, 26)
        panel_5.add(label_13)
        val q1 = new JTextArea
        q1.setBounds(20, 48, 292, 114)
        panel_5.add(q1)
        val q2 = new JTextArea
        q2.setBounds(378, 54, 292, 114)
        panel_5.add(q2)
        val q3 = new JTextArea
        q3.setBounds(20, 241, 292, 114)
        panel_5.add(q3)
        val q4 = new JTextArea
        q4.setBounds(392, 241, 292, 114)
        panel_5.add(q4)
        //*****************************************************************************


//------------------------------------------------------------DBLOAD-----------------------------------------------------
        var source=comboBox.getSelectedItem.toString

        val spark = SparkSession.builder()
          .appName("SSB")
          .master("local[*]")
          .config( "spark.driver.host", "localhost" )
          .getOrCreate()

        if(source=="mongodb") {

          // Read the data from MongoDB to a DataFrame
          val chemincnx = "."
          //------------------------------Supplier------------------------------
          val readConfigSupplier = ReadConfig(Map("uri" -> chemincnx, "database" -> "StarSchemaBenchmark(SF=1)", "collection" -> "Supplier"))
          val dfSupplier = spark.read.mongo(readConfigSupplier)
          dfSupplier.createOrReplaceTempView("Supplier")
          //------------------------------Customer------------------------------
          val readConfigCustomer = ReadConfig(Map("uri" -> chemincnx, "database" -> "StarSchemaBenchmark(SF=1)", "collection" -> "Customer"))
          val dfCustomer = spark.read.mongo(readConfigCustomer)
          dfCustomer.createOrReplaceTempView("Customer")
          //------------------------------Part------------------------------
          val readConfigPart = ReadConfig(Map("uri" -> chemincnx, "database" -> "StarSchemaBenchmark(SF=1)", "collection" -> "Part"))
          val dfPart = spark.read.mongo(readConfigPart)
          dfPart.createOrReplaceTempView("Part")
          //------------------------------Date------------------------------
          val readConfigDate = ReadConfig(Map("uri" -> chemincnx, "database" -> "StarSchemaBenchmark(SF=1)", "collection" -> "Date"))
          val dfDate = spark.read.mongo(readConfigDate)
          dfDate.createOrReplaceTempView("Date")
          //------------------------------LineOrder------------------------------
          val readConfigLineOrder = ReadConfig(Map("uri" -> chemincnx, "database" -> "StarSchemaBenchmark(SF=1)", "collection" -> "LineOrder"))
          val dfLineOrder = spark.read.mongo(readConfigLineOrder)
          dfLineOrder.createOrReplaceTempView("LineOrder")

        }
        else{
          val dfSupplier = spark.read.json("D:\\hadoop-2.7.1\\HDFS\\Supplier.json")
          dfSupplier.createOrReplaceTempView("Supplier")
          val dfCustomer = spark.read.json("D:\\hadoop-2.7.1\\HDFS\\Customer.json")
          dfCustomer.createOrReplaceTempView("Customer")
          val dfPart = spark.read.json("D:\\hadoop-2.7.1\\HDFS\\Part.json")
          dfPart.createOrReplaceTempView("Part")
          val dfDate = spark.read.json("D:\\hadoop-2.7.1\\HDFS\\Date.json")
          dfDate.createOrReplaceTempView("Date")
          val dfLineOrder = spark.read.json("D:\\hadoop-2.7.1\\HDFS\\LineOrder.json")
          dfLineOrder.createOrReplaceTempView("LineOrder")
        }
        println("***********************")
//-------------------------------------------------QGEN---------------------------------------------------------

        val chemincnx = "mongodb://127.0.0.1:27017/?gssapiServiceName=mongodb"
        //------------------------------Supplier------------------------------
        val writeconfig = ReadConfig(Map("uri" -> chemincnx, "database" -> "StarSchemaBenchmark", "collection" -> "Supplier"))

        //----------------------------query1----------------------------------------------

        var query1_1=spark.sql("select sum(LO_EXTENDEDPRICE*LO_DISCOUNT) as revenue from LineOrder , Date  " +
          "where LO_ORDERDATE =D_DATEKEY " +
          "and D_YEAR = "+yearQ1.getText().toInt +" and " +
          "LO_DISCOUNT between "+textField_4.getText().toInt+" and "+discountValMax.getText().toInt +" and " +
          "LO_QUANTITY < "+quantity.getText().toInt)
        q1.append("[REVENUE] \n")
        var xq1=query1_1.select("revenue").collect()
        println("****"+xq1.length+"--------------"+xq1.size)
        q1.append(""+xq1(0)+"\n")




        //----------------------------query2----------------------------------------------

        var query2=spark.sql("select sum(LO_REVENUE) as revenue , D_YEAR, P_BRAND1 from LineOrder , Date , Part , Supplier  " +
          "where LO_ORDERDATE= D_DATEKEY " +
          "and LO_PARTKEY = P_PARTKEY " +
          "and LO_SUPPKEY = S_SUPPKEY " +
          "and P_CATEGORY ='"+categoryQ2.getText() +
          "' and S_REGION = '"+regionSupplierQ2.getText()+
          "' group by D_YEAR, P_BRAND1 " +
          "order by D_YEAR, P_BRAND1")


        q2.append("[REVENUE,D_YEAR,P_BRAND1]\n")
         var xq2=query2.select("revenue","D_YEAR","P_BRAND1").collect()
       println("****"+xq2.length+"--------------"+xq2.size)
        for(i<-0 to xq2.length-1){
         q2.append(""+xq2(i)+"\n")
        }

        //----------------------------query3----------------------------------------------

        var query3=spark.sql("select C_NATION as C_NATION, S_NATION AS S_NATION, D_YEAR, sum(LO_REVENUE) as revenue " +
          "from Customer , LineOrder , Supplier , Date  " +
          "where LO_CUSTKEY = CUSTKEY  " +
          "and LO_SUPPKEY = S_SUPPKEY " +
          "and LO_ORDERDATE =D_DATEKEY " +
          "and C_REGION = '"+regionCustomerQ3.getText()+"' and S_REGION ='" +regionSupplierQ3.getText()+
          "' and D_YEAR >= "+yearDebut.getText().toInt+" and D_YEAR <= " +yearFin.getText().toInt+
          " group by C_NATION, S_NATION, D_YEAR " +
          "order by D_YEAR asc, revenue desc")
        q3.append("[C_NATION,S_NATION,D_YEAR,Revenue]\n")
        var xq3=query3.select("C_NATION","S_NATION","D_YEAR","revenue").collect()
        println("****"+xq3.length+"--------------"+xq3.size)
        for(i<-0 to xq3.length-1){
          q3.append(""+xq3(i)+"\n")
        }
        //----------------------------query4----------------------------------------------

        var query4=spark.sql("select D_YEAR, C_NATION,  sum(LO_REVENUE - LO_SUPPLYCOST) as profit " +
          "from Date , Customer , Supplier , Part , LineOrder  " +
          "where LO_CUSTKEY = C_CUSTKEY " +
          "and LO_SUPPKEY = S_SUPPKEY " +
          "and LO_PARTKEY = P_PARTKEY " +
          "and LO_ORDERDATE =D_DATEKEY  " +
          "and C_REGION ='"+textField.getText()+"' and S_REGION = '"+textField_1.getText()+
          "' and (P_MFGR = '"+textField_2.getText()+"' or P_MFGR = '"+textField_3.getText()+
          "' ) group by D_YEAR, C_NATION " +
          "order by D_YEAR, C_NATION ")
        q4.append("[D_YEAR, C_NATION,PROFIT]\n")
        var xq4=query4.select("D_YEAR", "C_NATION","profit").collect()
        println("****"+xq4.length+"--------------"+xq4.size)
        for(i<-0 to xq4.length-1) {
          q4.append("" + xq4(i) + "\n")
        }
        println("fin")

      }
    })

    btnExecuter.setFont(new Font("Tahoma", Font.BOLD, 14))
    btnExecuter.setBounds(382, 409, 122, 27)
    panel_4.add(btnExecuter)
   /* val lblNewLabel: JLabel = new JLabel("")
    lblNewLabel.setIcon(
      new ImageIcon("C:\\Users\\Boulqres\\Pictures\\Camera Roll\\back.PNG"))
    lblNewLabel.setBackground(new Color(102, 205, 170))
    lblNewLabel.setBounds(88, -29, 696, 458)
    panel.add(lblNewLabel)*/
    generation_chargement.addItemListener(new ItemListener() {
      def itemStateChanged(arg0: ItemEvent): Unit = {
        btngenerer.setVisible(false)
        btnCharger.setVisible(false)
        btnGnrerCharger.setVisible(true)
      }
    })
    generation.addItemListener(new ItemListener() {
      def itemStateChanged(arg0: ItemEvent): Unit = {
        btngenerer.setVisible(true)
        btnCharger.setVisible(false)
        btnGnrerCharger.setVisible(false)
      }
    })
    val bg2: ButtonGroup = new ButtonGroup()
    bg2.add(generation)
    bg2.add(generation_chargement)





  }
}