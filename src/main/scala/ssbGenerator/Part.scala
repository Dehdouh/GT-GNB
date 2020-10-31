package ssbGenerator

class Part {

  def getPartKey(partkey:Int):Int={
    return partkey+1
  }
  //----------------------------------------------------------------------
  def getMFGR(valeur:Int) : String={
    return "MFGR#"+valeur
  }
  //--------------------------------------------------------------
  def getCategory(valeur1:Int,valeur2:Int) : String={

    return "MFGR#"+valeur1+valeur2
  }
  //------------------------------------------------------------------
  def getBRAND1(valeur1:Int,valeur2:Int) : String={
    var x=new Methodes
    return getCategory(valeur1,valeur2)+x.getValeurAleatoire(1,6)+x.getValeurAleatoire(1,6)
  }
  //---------------------------------------------------------------------
  def getColor : String={
    val couleur=Array("almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue",
      "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral",
      "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick",
      "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew",
      "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen",
      "magenta", "maroon", "medium",  "metallic", "midnight", "mint", "misty", "moccasin", "navajo",
      "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder",
      "puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna",
      "sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet",
      "wheat", "white", "yellow")
    var valeur=new Methodes
    return couleur(valeur.getValeurAleatoire(1,90))
  }
  //----------------------------------------------------------------------------------
  def  getName:String={
    var couleur=new Part
    return couleur.getColor+" "+couleur.getColor
  }
  //---------------------------------------------------------------------------------
  def getType : String={
    val Syllable1 = Array("STANDARD","SMALL","MEDIUM","LARGE","ECONOMY","PROMO")
    val Syllable2 = Array("ANODIZED","BURNISHED","PLATED","POLISHED","BRUSHED")
    val Syllable3 =Array("TIN","NICKEL","BRASS","STEEL","COPPER")
    var valeur=new Methodes
    return Syllable1(valeur.getValeurAleatoire(0,6))+" "+Syllable2(valeur.getValeurAleatoire(1,6)-1)+" "+Syllable3(valeur.getValeurAleatoire(1,6)-1)
  }
  //---------------------------------------------------------------------------------
  def getSize: Int={
    var valeur=new Methodes
    return valeur.getValeurAleatoire(1,50)
  }
  //----------------------------------------------------------------------------------
  def getContainer:String ={
    val Syllable1 =Array("SM","LG","MED","JUMBO","WRAP")
    val Syllable2 =Array("CASE","BOX","BAG","JAR","PKG","PACK","CAN","DRUM")
    var valeur=new Methodes
    return Syllable1(valeur.getValeurAleatoire(1,6)-1)+" "+Syllable2(valeur.getValeurAleatoire(0,7))
  }
}

