# use-case-data-processing

## Run
In order to run the container, you will need ```docker``` installed in your machine:
- For MacOS, go to [Docker Hub](https://hub.docker.com/editions/community/docker-ce-desktop-mac/).
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
docker cp spark:/usr/src/app/output .
```
You will find the output .csv file in *output* folder in your project directory.

### Run tests
Unit testing is available with ```pytest``` for the batch processing code included in the *app*. You can run the unit tests in the container:
```bash
docker exec -it spark bash -c "python3 -m pytest tests.py"
```
Test results will be displayed on your terminal.
