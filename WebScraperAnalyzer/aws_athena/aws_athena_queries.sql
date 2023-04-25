
-- Create external table to access data in S3
CREATE EXTERNAL TABLE web_scraper_data (
   id INT,
   title STRING,
   category STRING,
   description STRING,
   page_url STRING,
   created_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://your-bucket-name/web_scraper_data/';

-- Query to count the number of rows in the table
SELECT COUNT(*) FROM web_scraper_data;

-- Query to calculate the average length of the description field
SELECT AVG(LENGTH(description)) AS avg_description_length FROM web_scraper_data;
