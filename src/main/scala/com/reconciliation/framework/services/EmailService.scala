package com.reconciliation.framework.services

import com.reconciliation.framework.core.AppConfig
import com.reconciliation.framework.sinks.Logger

import java.util.Properties
import javax.mail._
import javax.mail.internet._

object EmailService extends Logger {
  def sendEmail(config: AppConfig, subject: String, body: String): Unit = {
    if (config.notifyOnSuccess || config.notifyOnFailure) {
      config.emailTo.foreach { recipients =>
        val props = new Properties()
        props.put("mail.smtp.host", "your_smtp_host") // configure your smtp host
        props.put("mail.smtp.port", "587") // configure your smtp port
        props.put("mail.smtp.auth", "true")
        props.put("mail.smtp.starttls.enable", "true")

        val session = Session.getInstance(props, new Authenticator {
          override def getPasswordAuthentication: PasswordAuthentication = {
            new PasswordAuthentication("your_email", "your_password")
          }
        })

        try {
          val message = new MimeMessage(session)
          message.setFrom(new InternetAddress("your_email"))
          message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(recipients))
          message.setSubject(subject)
          message.setContent(body, "text/html")

          Transport.send(message)
          logger.info(s"Email sent to $recipients")
        } catch {
          case e: MessagingException =>
            logger.error(s"Failed to send email: ${e.getMessage}", e)
        }
      }
    }
  }
}
