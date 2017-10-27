# Google/Bing Images Scraper

Built with [serverless framework](https://github.com/serverless/serverless).

Serverless app on AWS that scrapes Google Images for given queries. 

Uses AWS Lambda to run code, DynamoDB for intermediate storage and S3 to save scraped images.

Usage:

1. Deploy to AWS with 
``` 
sls deploy
```
2. Insert queries to scrape images using [`insert_queries.ipynb`](./insert_queries.ipynb).

3. Give it some time. You can check the number of items in `download_from_s3` table to review the progress.

3. Download images from S3 using [`download_from_s3.ipynb`](./download_from_s3.ipynb).


