package com.reconciliation.model

case class ReconConfig(
    jobName: String,
    sourceType: String,
    sourceLocation: String,
    targetType: String,
    targetLocation: String,
    columnMapping: String,
    keyMapping: String,
    reconType: String,
    sqlQuery: String,
    emailTo: String,
    emailCc: String,
    notifyOnSuccess: String,
    notifyOnFailure: String
)
