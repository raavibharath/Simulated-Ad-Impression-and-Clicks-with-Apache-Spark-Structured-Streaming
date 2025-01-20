# Simulated-Ad-Impression-and-Clicks-with-Apache-Spark-Structured-Streaming
This repository contains an implementation of simulated advertisement impressions and clicks using Apache Spark's Structured Streaming API. The project aims to showcase how to process and analyze real-time streaming data to identify meaningful user interactions, particularly focusing on impressions and clicks that occur within a relevant time window.

# Features
Stream Simulation: Simulates real-time advertisement impressions and user clicks using the rate source in Spark.
Stream-Stream Join: Joins impression and click streams based on the adId to detect meaningful interactions.
Time-Based Filtering: Filters interactions based on time constraints (5-second window) to determine if a click is "meaningful."
Real-Time Analytics: Uses Spark Structured Streaming to continuously process and output data, identifying valid interactions in real time.

# How It Works
Simulating Streams: The project uses Apache Spark’s rate source to generate two streams: one for advertisement impressions and another for clicks. The impression stream is produced at 5 rows per second, and the click stream is simulated for 10% of impressions.

Stream Joining: A stream-stream join is performed between the impressions and clicks based on a shared adId. A time window of ±5 seconds around the impression time ensures that clicks are meaningful.

Real-Time Output: The project is set up to run in append mode, continuously processing and outputting meaningful click events to the console.

# Tools & skills:
Databricks & Apache spark, Streaming




