package ssbGenerator

import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

class Date {
  def getDATEKEY(cal:Calendar) : String ={
    return ""+cal.get(Calendar.YEAR)+(cal.get(Calendar.MONTH)+1)+cal.get(Calendar.DAY_OF_MONTH)
  }
  def getDATE(cal:Calendar) : String ={
    return ""+cal.getDisplayName(Calendar.MONTH,Calendar.LONG, Locale.US)+cal.get(Calendar.DAY_OF_MONTH)+","+cal.get(Calendar.YEAR)
  }
  def getDAYOFWEEK(cal:Calendar) : String ={
    return ""+cal.getDisplayName(Calendar.DAY_OF_WEEK,Calendar.LONG, Locale.US)
  }
  def getMONTH(cal:Calendar) : String ={
    return ""+cal.getDisplayName(Calendar.MONTH,Calendar.LONG, Locale.US)
  }
  def getYEAR(cal:Calendar) : String ={
    return ""+cal.get(Calendar.YEAR)
  }
  def getYEARMONTHNUM(cal:Calendar) : String ={
    return ""+cal.get(Calendar.YEAR)+(cal.get(Calendar.MONTH)+1)
  }
  def getYEARMONTH(cal:Calendar) : String ={
    return ""+cal.getDisplayName(Calendar.MONTH,Calendar.SHORT, Locale.US)+cal.get(Calendar.YEAR)
  }
  def getDAYNUMINWEEK(cal:Calendar) : String ={
    return ""+cal.get(Calendar.DAY_OF_WEEK)
  }
  def getDAYNUMINMONTH(cal:Calendar) : String ={
    return ""+cal.get(Calendar.DAY_OF_MONTH)
  }
  def getDAYNUMINYEAR(cal:Calendar) : String ={
    return ""+cal.get(Calendar.DAY_OF_YEAR)
  }
  def getMONTHNUMINYEAR(cal:Calendar) : String ={
    return ""+(cal.get(Calendar.MONTH)+1)
  }
  def getWEEKNUMINYEAR(cal:Calendar) : String ={
    return ""+cal.get(Calendar.WEEK_OF_YEAR)
  }
  def getSELLINGSEASON(cal:Calendar) : String ={
    var  datecal = cal.getTime()
    var format = new SimpleDateFormat("dd-MM")
    var formatted = format.format(cal.getTime())
    var date1=format.parse(format.format(cal.getTime()))
    if((date1.after(format.parse("31-10")))||(date1.before(format.parse("01-01")))){
      return("Christmas")
    }else {if((date1.after(format.parse("30-04")))&&(date1.before(format.parse("01-09")))){
      return("Summer")
    }else if((date1.after(format.parse("31-08")))&&(date1.before(format.parse("01-11")))){
      return("Fall")
    }else if((date1.after(format.parse("31-03")))&&(date1.before(format.parse("01-05")))){
      return("Spring")
    }  else{
      return("Winter")
    }}
  }
  def getHOLIDAYFL(cal:Calendar) : Int ={
    var datecal = cal.getTime()
    var format = new SimpleDateFormat("dd-MM")
    var formatted = format.format(cal.getTime())
    if (formatted.equals("24-12")||formatted.equals("01-01")||formatted.equals("20-02")||formatted.equals("20-04")
      ||formatted.equals("20-05")||formatted.equals("20-07")||formatted.equals("20-08")||formatted.equals("20-09")
      ||formatted.equals("20-10")||formatted.equals("20-11")){
      return 1;}
    else{
      return 0;}
  }
  def getLastDayInWeekFL(cal:Calendar) : Int ={
    if(cal.get(Calendar.DAY_OF_WEEK)==cal.getMaximum(Calendar.DAY_OF_WEEK))
    { return 1;}
    else{
      return 0;
    }
  }
  def getLastDayInMonthFL(cal:Calendar) : Int ={
    if(cal.get(Calendar.DAY_OF_MONTH)==cal.getMaximum(Calendar.DAY_OF_MONTH))
    { return 1;}
    else{
      return 0;
    }
  }
  def getWEEKDAYFL(cal:Calendar) : Int ={
    if((cal.get(Calendar.DAY_OF_MONTH)==cal.getMaximum(Calendar.DAY_OF_MONTH))||(cal.get(Calendar.DAY_OF_MONTH)==cal.getMaximum(Calendar.DAY_OF_MONTH)-1))
    {return 1;
    } else{
      return 0;
    }
  }
}


