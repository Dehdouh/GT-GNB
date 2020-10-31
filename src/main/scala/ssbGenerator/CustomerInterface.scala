package ssbGenerator

import java.awt.{Color, EventQueue, Font, SystemColor}
import java.awt.event.{ActionEvent, ActionListener}

import javax.swing._


object CustomerInterface {

  /**
    * Launch the application.
    */
  def main(args: Array[String]): Unit = {
    EventQueue.invokeLater(new Runnable() {
      def run(): Unit = {
        try {
          val window: CustomerInterface = new CustomerInterface()
          window.frame.setVisible(true)
        } catch {
          case e: Exception => e.printStackTrace()

        }
      }
    })
  }

}

class CustomerInterface {

  var frame: JFrame = _

  initialize()

  /**
    * Initialize the contents of the frame.
    */
  private def initialize(): Unit = {
    frame = new JFrame("Customer")
    frame.setBounds(100, 100, 550, 465)
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
    frame.getContentPane.setLayout(null)
    val panel: JPanel = new JPanel()
    panel.setBackground(SystemColor.textHighlightText)
    panel.setBounds(0, 0, 584, 425)
    frame.getContentPane.add(panel)
    panel.setLayout(null)
   /* val label: JLabel = new JLabel("")
    label.setIcon(
      new ImageIcon("C:\\Users\\Boulqres\\Pictures\\Camera Roll\\bachh.PNG"))
    label.setBounds(0, 0, 535, 55)
    panel.add(label)*/
    val btnNewButton: JButton = new JButton("Cancel")
    btnNewButton.addActionListener(new ActionListener() {
      def actionPerformed(arg0: ActionEvent): Unit = {
        frame.setVisible(false)
      }
    })
    btnNewButton.setFont(new Font("Tahoma", Font.BOLD | Font.ITALIC, 16))
    btnNewButton.setForeground(new Color(0, 0, 0))
    btnNewButton.setBackground(SystemColor.control)
    btnNewButton.setBounds(411, 379, 105, 35)
    panel.add(btnNewButton)
    val lblNewLabel: JLabel = new JLabel("")
    lblNewLabel.setIcon(
      new ImageIcon("C:\\Users\\pc bouinan\\Documents\\GénérateurSSB_NOSQL\\Pictures\\cust.PNG"))
    lblNewLabel.setBounds(0, 63, 506, 340)
    panel.add(lblNewLabel)
  }

}

