package com.reconciliation.framework.services

import com.reconciliation.framework.core.AppConfig
import com.reconciliation.framework.sinks.Logger

import java.util.Properties
import javax.mail._
import javax.mail.internet._

object EmailService extends Logger {
  def sendEmail(config: AppConfig, subject: String, body: String): Unit = {
    val props = new Properties()
    props.put("mail.smtp.host", config.emailSmtpHost)
    props.put("mail.smtp.port", config.emailSmtpPort.toString)

    config.emailUsername.foreach { username =>
      props.put("mail.smtp.auth", "true")
      val authenticator = new Authenticator() {
        override def getPasswordAuthentication: PasswordAuthentication = {
          new PasswordAuthentication(username, config.emailPassword.getOrElse(""))
        }
      }
      val session = Session.getInstance(props, authenticator)
      sendMessage(session, config, subject, body)
    }

    if (config.emailUsername.isEmpty) {
      val session = Session.getInstance(props)
      sendMessage(session, config, subject, body)
    }
  }

  private def sendMessage(session: Session, config: AppConfig, subject: String, body: String): Unit = {
    try {
      val message = new MimeMessage(session)
      message.setFrom(new InternetAddress(config.emailUsername.getOrElse("noreply@example.com")))
      config.emailTo.foreach { to =>
        message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to))
      }
      message.setSubject(subject)
      message.setContent(body, "text/html; charset=utf-8")
      Transport.send(message)
      logger.info("Email sent successfully.")
    } catch {
      case e: MessagingException =>
        logger.error(s"Failed to send email: ${e.getMessage}")
    }
  }
}
