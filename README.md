# Dockerized version of counting word frequency through MapReduce in Apache Airflow.
### Prerequisites
* Docker19.03 or greater
* Docker-compose 1.26 or greater
### Files definitions:

- [dags](dags) - Path with DAG file
- [plugins](plugins) - Path with custom operators(MapOperator and ReduceOperator)
- [input_data/tweets.csv](input_data/tweets.csv) - Tweets Dataset - Top 20 most followed users in Twitter social platform
- [docker-compose.yaml](docker-compose.yaml) - Docker-compose file
- [requirements.txt](requirements.txt) - File with additional dependencies which will be installed in docker


## To launch:

```Linux Shell
    docker-compose up --build -d airflow-init
    sudo chmod -R 777 minio
    docker-compose up
```

During `docker-compose up` you need to press `Ctrl-C` , run `sudo chmod -R 777 minio` and then again 
`docker-compose up`. This is because you need to give a permission to a minio folder.