class EmailAlert extends Alert {
  override def sendAlert(message: String): Unit = {
    // Implement email sending logic here
    // Example: Using a dummy print statement for illustration
    println(s"Alert:\n$message")
    
    // Example email sending implementation
    // val recipient = "recipient@example.com"
    // val subject = "Hive Query Breach Alert"
    // val body = message
    // EmailSender.send(recipient, subject, body)
  }
}
