import java.util.Properties
import javax.mail._
import javax.mail.internet._

object EmailSender {
  def send(to: String, subject: String, body: String): Unit = {
    val props = new Properties()
    props.put("mail.smtp.host", "smtp.example.com")
    props.put("mail.smtp.port", "587")
    props.put("mail.smtp.auth", "true")
    props.put("mail.smtp.starttls.enable", "true")

    val session = Session.getInstance(props, new Authenticator() {
      override protected def getPasswordAuthentication: PasswordAuthentication = {
        new PasswordAuthentication("your_email@example.com", "your_password")
      }
    })

    try {
      val message = new MimeMessage(session)
      message.setFrom(new InternetAddress("your_email@example.com"))
      message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to).asInstanceOf[Array[Address]])
      message.setSubject(subject)
      message.setText(body)

      Transport.send(message)
      println("Email sent successfully")
    } catch {
      case e: MessagingException => throw new RuntimeException(e)
    }
  }
}
