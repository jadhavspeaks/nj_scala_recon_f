package com.reconciliation.notification

import java.util.Properties
import javax.mail._
import javax.mail.internet._

object EmailService {

  def sendEmail(to: String, cc: String, subject: String, body: String): Unit = {
    val props = new Properties()
    props.put("mail.smtp.host", "your-smtp-server") // Replace with your SMTP server
    props.put("mail.smtp.port", "25") // Replace with your SMTP port

    val session = Session.getInstance(props, null)
    val message = new MimeMessage(session)
    message.setFrom(new InternetAddress("your-email@example.com")) // Replace with your email
    message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to))
    if (cc != null && cc.nonEmpty) {
      message.setRecipients(Message.RecipientType.CC, InternetAddress.parse(cc))
    }
    message.setSubject(subject)
    message.setContent(body, "text/html; charset=utf-8")

    Transport.send(message)
  }
}
