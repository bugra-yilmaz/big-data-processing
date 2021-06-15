# use-case-data-processing

## Run
In order to run the container, you will need ```docker``` installed in your machine:
- For macOS, go to [Docker Hub](https://hub.docker.com/editions/community/docker-ce-desktop-mac/).
- For Ubuntu, go to [Install Docker Engine on Ubuntu](https://docs.docker.com/engine/install/ubuntu/).

Then, first, build the Docker image:
```bash
docker build -t spark .
```

Start the container:
```bash
docker run --name spark -d -t spark
```

### Run processing job
In order to run the processing job in the container, you can execute the command below. Please specify your own input parameters.
```bash
docker exec -it spark bash -c "spark-submit main.py -p 'news, movies' -m 'fre, dur' -t '365, 730, 1460, 2920' -d '12/10/2019'"
```
This will produce the requested output file in the container.

You can copy the output file to your local machine:
```bash
docker cp spark:/usr/src/app/output.csv .
```

### Run tests
Unit testing is available with ```pytest``` for the batch processing code included in the *app*. You can run the unit tests in the container:
```bash
docker exec -it spark bash -c "python3 -m pytest tests.py"
```
Test results will be displayed on your terminal.

## Approach

#### Insights from dataset
The first thing we will notice about the sample data is that big majority of users have very few actions. More precisely, there are ```435``` users in the dataset, ```401``` of them (```92%```) have only a single event. This also means that, at the end of the analysis, we will see that most of the users have meaningful values for only one page type.

A similar unbalanced distribution is present in event dates. We have data between years 2015 and 2019 (both included). However, ```78%``` of the events belong to the year 2016 whereas the year 2019 has only one event.

When we look at the page type distribution, we see that ```news``` type is too dominant. ```movies``` consists of only ```2.6%``` of the user events.

#### Partitioning strategy
For the sake of this assignment, since the computation forces us to group user ids in worker nodes, partitioning the dataset by the user ids made the most sense. Another partitioning strategy may result in a costly reshuffling of data when the *groupby* command is executed.

However, we should keep in mind the fact that most of the users have very few actions. So, we need to avoid ending up with too many partitions which will drastically hurt Spark's performance. Setting the number of partitions equal to the number of available worker nodes and using a "hash partitioner" to distribute users equally to the worker nodes seemed like the right approach here. During a test run, with 4 partitions, I had ```112```, ```109```, ```97``` and ```117``` users in partitions respectively, which can be considered as a balanced distribution.

Another point that is worth to be mentioned is whether we have an established partitioning schema on disk. For this kind of use case, we would probably accumulate new data every day. So, the best practice for partitioning the data on disk would be partitioning it by date, e.g. ```yyyy/yyyy-mm-dd/``` or  ```yyyy-mm-dd/```. With this in mind, let's think about what happens when we partition the data by user ids in the Spark job, namely:
```python
df = df.repartition(n, 'USER_ID')
```

This will reshuffle the data by user ids. In fact, letting Spark do reshuffling by itself when it sees the *groupby* execution may result in a better performance considering Spark's inner optimization algorithms. In my opinion, the best approach would be to try both strategies with large datasets to see which one performs better for this use case.

#### Compare strategies with an augmented dataset
For getting an insight about how these strategies would compare in a real use case, I augmented the given data in ```fact.csv``` by repeating the rows with different user ids (randomly generated ones). This resulted in a .csv file with 262 M lines (~ 7 GB on disk).

I executed the Spark job on this file with the arguments specified in the assignment description. Average execution times from 5 runs per each option are below (note: tested with 8 cores on local machine):
- w/o partitioning by user id: ```209.3``` seconds.
- partitioning by user id, with 8 partitions: ```131.9``` seconds.
- partitioning by user id, with 16 partitions: ```117.8``` seconds.
- partitioning by user id, with 32 partitions: ```122.3``` seconds.
- partitioning by user id, with 64 partitions: ```123.9``` seconds.

Apparently, partitioning by user id improves the performance of Spark for this use case. The best performing case is partitioning by user id with ```16``` partitions (2x number of available cores). But, again, we need to keep in mind the fact that the results may differ when we use a remote Spark cluster and/or when we have an existing partitioning schema on disk.

## Further work
- Run the Spark app on a remote cluster. Databricks would be a good option.
- Measure performance with an existing, realistic partitioning schema on disk, e.g. partition ```fact.csv``` by date.
- Move the application to Scala Spark for performance tests.
- Bonus: Move the system to streaming.
