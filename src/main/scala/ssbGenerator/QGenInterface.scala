package ssbGenerator
import java.awt.EventQueue
import javax.swing.JFrame
import javax.swing.JPanel
import java.awt.Color
import javax.swing.JLabel
import javax.swing.ImageIcon
import java.awt.Font
import javax.swing.JButton
import java.awt.SystemColor
import java.awt.event.ActionListener
import java.awt.event.ActionEvent
import javax.swing.JTextArea
import QGenInterface._
//remove if not needed
import scala.collection.JavaConversions._
object QGenInterface {

  /**
    * Launch the application.
    */
  def main(args: Array[String]): Unit = {
    EventQueue.invokeLater(new Runnable() {
      def run(): Unit = {
        try {
          val window: QGenInterface = new QGenInterface()
          window.frame.setVisible(true)
        } catch {
          case e: Exception => e.printStackTrace()

        }
      }
    })
  }

}

class QGenInterface {

  var frame: JFrame = _

  initialize()
  /**
    * Initialize the contents of the frame.
    */
  private def initialize(): Unit = {
    frame = new JFrame()
    frame.setBounds(100, 100, 750, 600)
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
    frame.getContentPane.setLayout(null)
    val panel: JPanel = new JPanel()
    panel.setBackground(Color.WHITE)
    panel.setForeground(Color.BLACK)
    panel.setBounds(10, 11, 724, 561)
    frame.getContentPane.add(panel)
    panel.setLayout(null)
   /* val label: JLabel = new JLabel("")
    label.setIcon(
      new ImageIcon("C:\\Users\\Boulqres\\Pictures\\Camera Roll\\bachh.PNG"))
    label.setBounds(0, 0, 519, 104)
    panel.add(label)
    val label_1: JLabel = new JLabel("")
    label_1.setIcon(
      new ImageIcon("C:\\Users\\Boulqres\\Pictures\\Camera Roll\\bachh.PNG"))
    label_1.setBounds(495, 0, 519, 104)
    panel.add(label_1)*/
    val button: JButton = new JButton("Cancel")
    button.addActionListener(new ActionListener() {
      def actionPerformed(arg0: ActionEvent): Unit = {
        frame.setVisible(false)
      }
    })
    button.setForeground(Color.BLACK)
    button.setFont(new Font("Tahoma", Font.BOLD | Font.ITALIC, 16))
    button.setBackground(SystemColor.menu)
    button.setBounds(619, 530, 105, 26)
    panel.add(button)
    val lblQgenDtails: JLabel = new JLabel("QGen details")
    lblQgenDtails.setFont(new Font("Tahoma", Font.BOLD | Font.ITALIC, 25))
    lblQgenDtails.setBounds(257, 115, 325, 31)
    panel.add(lblQgenDtails)
    val label_2: JLabel = new JLabel("")
    label_2.setIcon(
      new ImageIcon("C:\\Users\\pc bouinan\\Documents\\GénérateurSSB_NOSQL\\Pictures\\qgen.PNG"))
    label_2.setBounds(95, 157, 532, 380)
    panel.add(label_2)
  }

}
