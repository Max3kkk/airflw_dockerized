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
### 1. Build containers and initialize Apache Aiflow:

```Linux Shell
    docker-compose up --build -d airflow-init
    docker-compose up
```
### 2. Grant permissions to folders created by minio and postgres 
```Linux Shell
    sudo chmod -R 777 minio
    sudo chmod -R 777 postgres-data
```
### 3. Add connection to minio in Apache AIflow Admin panel:
- go to `localhost:8080` login with (`user`: airflow, `password`: airflow)
- go to admin > connections
- add new connection with the following parameters:
```
    Coonn Id = local_minio
    Conn Type = S3

    Extra:
    {
        "aws_access_key_id":"minio-access-key",
        "aws_secret_access_key": "minio-secret-key",
        "host": "http://minio:9000"
    }
 ```


### 3. Add connection to postgres in Apache AIflow Admin panel:
- go to `localhost:8080` login with (`user`: airflow, `password`: airflow)
- go to admin > connections
- add new connection with the following parameters:
```
    Coonn Id = postrgre_sql
    Conn Type = Postgres
    Host: db
    Schema: tweets
    Login: postgres
    Password: postgres
 ```
### 4. Launch docker:
```Linux Shell
    docker-compose up
```