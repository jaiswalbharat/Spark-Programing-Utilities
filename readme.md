# Hive Query Executor and Validator

## Overview

This project aims to execute Hive queries, save results in a Hive table, and compare current results with previous iterations to detect any breaches based on a specified threshold. If breaches are detected, an alert email is sent summarizing all breaches.

## Prerequisites

- Apache Spark
- Apache Hive
- Scala
- JSON4S library for JSON parsing
- Email library (e.g., JavaMail for sending emails)

## Project Structure

- `src/main/scala/SampleDataGenerator.scala`: Generates sample data and saves it to a Hive table.
- `src/main/scala/Driver.scala`: Main driver class that executes the queries, performs comparisons, and sends alerts.
- `src/main/scala/HiveQueryExecutor.scala`: Implements the `QueryExecute` interface for executing Hive queries.
- `src/main/scala/EmailAlert.scala`: Implements the `Alert` interface for sending email alerts.
- `src/main/scala/HiveTableValidation.scala`: Implements the `Validation` interface for validating Hive tables.
- `src/main/scala/EmailSender.scala`: Optional class for sending emails using JavaMail.

## JSON Configuration

The JSON configuration file defines the queries and their parameters:

```json
[
  {
    "database": "example_db",
    "query": "SELECT group_by_column, COUNT(*) as row_count FROM example_db.results WHERE date = '2024-07-12' GROUP BY group_by_column",
    "group_by_column": "group_by_column",
    "compare_iteration": 5,
    "threshold_percentage": 20
  }
]
