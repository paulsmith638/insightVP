# insightVP
Consulting Project with Vinepair.com for Insight Data Science, Fall 2019

![Pipeline](/pipeline.jpg)
The Vinepair.com website receives 3-5 pageviews per month. The company wishes to collect and analyze web-traffic data to direct content generation decisions and to market to outside data consumers.

I have implemented the two-stage pipeline shown above to address these needs.  This pipeline consists of a data collection and storage back-end and an analysis pipeline frond-end. The back-end pulls pageview, event, and search data from Google Analytics and web taxonomy directly from the Vinepair CMS database. These data are stored in an AWS-EC2-hosted MySQL database. The data analysis pipeline queries this database to extract, de-noise, and aggregate time series data. These data are then pushed to both Plotly graphs and JSON files for ingestion into the Vinepair CMS.  Trend modeling employs ARIMA approaches while the noise filtering and page weighting schemes employed are propriatary and are removed from this repository.
