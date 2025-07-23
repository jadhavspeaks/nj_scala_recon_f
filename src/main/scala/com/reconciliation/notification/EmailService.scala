package com.reconciliation.notification

import java.util.Properties
import javax.mail._
import javax.mail.internet._
import com.reconciliation.model.ReconConfig
import org.apache.spark.sql.DataFrame

object EmailService {

  def sendEmail(config: ReconConfig, status: String, mismatchCount: Long, totalTime: Long, mismatchDF: DataFrame): Unit = {
    val props = new Properties()
    props.put("mail.smtp.host", "your-smtp-server") // Replace with your SMTP server
    props.put("mail.smtp.port", "25") // Replace with your SMTP port

    val session = Session.getInstance(props, null)
    val message = new MimeMessage(session)
    message.setFrom(new InternetAddress("your-email@example.com")) // Replace with your email
    message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(config.emailTo))
    if (config.emailCc != null && config.emailCc.nonEmpty) {
      message.setRecipients(Message.RecipientType.CC, InternetAddress.parse(config.emailCc))
    }
    message.setSubject(s"Reconciliation Report: ${config.jobName} - $status")
    message.setContent(generateEmailBody(config, status, mismatchCount, totalTime, mismatchDF), "text/html; charset=utf-8")

    Transport.send(message)
  }

  private def generateEmailBody(config: ReconConfig, status: String, mismatchCount: Long, totalTime: Long, mismatchDF: DataFrame): String = {
    val statusColor = if (status == "SUCCESS") "green" else "red"
    val mismatchPreview = if (mismatchCount > 0) {
      val previewData = mismatchDF.limit(10).collect()
      val headers = mismatchDF.columns.map(h => s"<th>$h</th>").mkString
      val rows = previewData.map(row => "<tr>" + row.toSeq.map(c => s"<td>$c</td>").mkString + "</tr>").mkString
      s"""
        <h3>Mismatch Preview (Top 10)</h3>
        <table border="1" style="width:100%; border-collapse: collapse;">
          <thead>
            <tr>$headers</tr>
          </thead>
          <tbody>
            $rows
          </tbody>
        </table>
      """
    } else ""

    s"""
      |<html>
      |<head>
      |<style>
      |  body { font-family: Arial, sans-serif; }
      |  .header { background-color: #4CAF50; color: white; padding: 10px; text-align: center; }
      |  .content { padding: 20px; }
      |  .summary-table { border-collapse: collapse; width: 100%; }
      |  .summary-table td, .summary-table th { border: 1px solid #ddd; padding: 8px; }
      |  .summary-table tr:nth-child(even){background-color: #f2f2f2;}
      |  .summary-table th { padding-top: 12px; padding-bottom: 12px; text-align: left; background-color: #4CAF50; color: white; }
      |  .status { font-weight: bold; color: $statusColor; }
      |</style>
      |</head>
      |<body>
      |  <div class="header">
      |    <h1>Reconciliation Report</h1>
      |  </div>
      |  <div class="content">
      |    <h2>Job: ${config.jobName}</h2>
      |    <table class="summary-table">
      |      <tr>
      |        <th>Parameter</th>
      |        <th>Value</th>
      |      </tr>
      |      <tr>
      |        <td>Job Name</td>
      |        <td>${config.jobName}</td>
      |      </tr>
      |      <tr>
      |        <td>Reconciliation Type</td>
      |        <td>${config.reconType}</td>
      |      </tr>
      |      <tr>
      |        <td>Status</td>
      |        <td class="status">${status}</td>
      |      </tr>
      |      <tr>
      |        <td>Mismatch Count</td>
      |        <td>${mismatchCount}</td>
      |      </tr>
      |      <tr>
      |        <td>Total Time Taken</td>
      |        <td>${totalTime} seconds</td>
      |      </tr>
      |    </table>
      |    ${mismatchPreview}
      |  </div>
      |</body>
      |</html>
    """.stripMargin
  }
}
