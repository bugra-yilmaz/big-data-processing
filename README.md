# use-case-data-processing

Write a process to generate RF (recency, frequency) metrics for page views, aggregated by user, using the attached datasets.

##### Assume:

- fact.csv to be bigger than 500Gb
- lookup.csv to be a static, small file

##### For you to decide:

- develop app using (PySpark/ScalaSpark)
- partition strategy
- data processing approach

##### Have in mind:

- number of time windows may increase
- pagetype could include new types in the future
- processing historical data ocurres often
- code/app delivered should be such that it can be easily productionized
- code/app should generate the output in csv format

##### Input parameters to be passed to the app

- pagetype – 'news, movies'
- metrictype – 'fre, dur'
- timewindow – '365, 730, 1460, 2920'
- dateofreference - '12/10/2019'

##### Outputs given by the process (a file like below) with around 10 metrics e.g.

| user_id | pageview_news_dur | pageview_news_fre_365 | pageview_news_fre_730 | ...|
| ------- | ----------------- | --------------------- | --------------------- | -- |

##### Output metrics naming convention to be followed:

`pageview_<pagetype>_<metrictype>_<timewindow>`


## Reference

- fre - frequency of page views (count)
- dur - recency of page views (no. of days since the last page view from date of reference)

##### Metric definitions:

- pageview_news_fre_365 - no of page views for news page type in last 365 days
- pageview_news_dur_730 - recency of page views for news category in last 365 days
