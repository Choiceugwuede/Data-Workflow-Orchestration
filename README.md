# CoreSentiment Model

# Project Background

CoreSentiment project was initiated by a data consulting organization developing a stock market prediction tool that leverages public sentiment signals
derived from Wikipedia pageviews.

## The idea:
- An increase in a company's Wikipedia pageviews indicates growing public interest, i.e. positive market sentiment and a likely increase in stock price.
- A decrease suggests low attention, i.e. a negative market sentiment, and a potential decrease in stock price.

To operationalize this idea, the organization required a reliable, automated data pipeline to:
- Continuously read hourly Wikipedia pageview data for selected technology companies
- Ingest , process, and store this data
- Enable analysis for sentiment classiciation.

# The solution 

I designed and implemented a fully orchestrated data pipeline using Apache Airflow that automates the end-to-end process of collecting, processing, and analyzing Wikipedia pageviews for 
selected companies - Google, Microsoft, Amazon, Apple, and Facebook.

# Architecture

<img width="3164" height="1448" alt="image" src="https://github.com/user-attachments/assets/b2d19f7d-4140-472b-a107-055da66c9875" />

## Key Features
- Dynamically parses through Wikipedia pageview dumps.
- Extracts and transforms relevant company data using Python.
- Stores processed data in a Database for analytics.
- Analyzes hourly data to identify the most viewed company per ingestion cycle (a sentiment indicator)
- Sends automated email notifications of current most viewed company after each successful run.

This workflow forms the foundation for the organization's CoreSentiment model, which can be scaled or extended to improve prediction acuracy and operational reliability.

# Tools 
- Airflow 3.1.0 (Dockerized)
- PostgreSQL (Dockerized)
- Python: for data extraction and transformation
- BeautifulSoup: Web scraping and dynamic file discovery (from the Wikipedia page dump)
- Airflow XCom: Task-to-task communications e.g output of extraction goes to loading etc.
- Smtp(EmailOperator): Email notifications for results and alerts.

 # Pipeline Steps:
 1. get_next_file
    - Dynamically scrapes the latest available hourly Wikipedia pageview dump from the site using BeautifulSoup.
    - Tracks process using local state file (last_processed.txt) to ensure no file is processed twice.
   
 2. extract_pageviews_hour
    - Downloads and unzips the .gz dump file containing details for that hour.
    - Filters for selected companies using Python and writes to CSV.
    - Pushes the output path via XCom for downstream tasks.

  3. load_to_postgres
     - Reads the CSV file and loads it into the PostgreSQL table (pageviews)
     - Uses Airflow's PostgresHook and SQLAlchemy engine for smooth database operations.

  4. get_top_company
     - Executes an SQL query also using PostgresHook to aggregate total views by company and identify the most viewd company.
     - Pushes the result (company + total views) to XCom.
    
  5. send_notification
     - Sends an automated email alert using Airflow's EmailOperator with configured SMTP connection, containing
       - Hourly file processed.
       - Top company viewed at that hour.
       - Total Number of views
       
# Key Improvement made:
Initial simple workflow was created to perform the extraction, process and storage - core-sentiment.

Which was then upgraded with more features for more productivity:
- Data disocvery: Added dynamic discovery of hourly files instead of static URLs.
- Data Integrity: Introduced state tracking for simple incremental ingestion.
- Analytics: Added top company SQL analysis task.
- Result notification: Configured email alerts for completion summary.

# How to RUN:
1. Clone Repository:
   ``` bash
   git clone https://github.com/Choiceugwuede/Data-Workflow-Orchestration.git
   cd Data-Workflow-Orchestration
   ```

2. Start Docker Containers: Ensure Docker is running, then execute:
   ``` bash
   docker-compose up
   ```

3. Configure Notifications
   Add your email address to get notifications
   
4. Access Airflow UI to monitor Dags run:
   - Navigate to http://localhost:8080
   - Log in (default: airflow/airflow)

5. Trigger the DAG:
   The DAG is scheduled to run daily, but you can test manually:
   - Locate core-sentiment-V2
   - Click trigger DAG

6. Check your mail.
   After a successful run, you'll receive an email showing the processed file, top company, and number of views.

Example Output (Email Notification)


