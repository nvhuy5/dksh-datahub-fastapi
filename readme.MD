# DataHub Codeflow
![image](./DataHub%20Code%20Flow.png)
# Local deployment
1. Credentials  
Before starting the project, please follow some steps to generate some local files to store neccessary credentials  

i. Create an `.env` file with content
```txt
COMPOSE_PROJECT_NAME=dksh-datahub
ENVIRONMENT=dev
BACKEND_HOST=***
BACKEND_PORT=8080
REDIS_HOST=***
REDIS_PORT=6379
APP_PORT=8000
FLOWER_PORT=5555
BASE_API_URL=https://dev-datahub.dksh.b2b.com.my
AWS_REGION=ap-southeast-1

```
*Notes: Must provide the `ENVIRONMENT` variable for every environment deployed (dev, qa, uat, etc.)*

ii. Create AWS credentials in current directory under a directory named `.aws`  
```
.  
|--- docker-compose.yml  
|--- .aws  
|   |--- config  
|   |--- credentials  
```

- The `config` file content likes:
```txt
[default]
region = ap-southeast-1

```

- The `credentials` file content likes:
```txt
[default]
aws_access_key_id = ***
aws_secret_access_key = ***
aws_session_token = ***

```
*Replace *** by the actual values*

2. Update the `./app/fastapi_celery/config.ini` file for AWS S3
```txt
[support_types]
types=[".pdf", ".txt", ".xml", ".doc", ".json", ".csv", ".xlsx", ".xls"]

[s3_buckets]
default_region=ap-southeast-1
datahub_s3_raw_data=dksh-datahub-__env__-s3-raw-data
datahub_s3_process_data=dksh-datahub-__env__-s3-process-data
datahub_s3_master_data=dksh-datahub-__env__-s3-master-data
materialized_step_data_loc=workflow-node-materialized

```

3. Initialize docker containers
```bash
sudo docker-compose up -d --build --force-recreate
# Show all docker containers
sudo docker ps -a
```

4. Debug the logic after development
=> Rebuild the running containers
```bash
sudo docker-compose up -d --build --force-recreate
```

5. Remove all docker containers and attached volumes if not using
```bash
# Stop and remove everything
# remove all built containers and existing images
sudo docker rm $(sudo docker ps -a -q) -f
sudo docker rmi $(sudo docker images -a -q)
# prune all volumes
sudo docker system prune -a --volumes
```

# Pytest
1. Structure of dksh-datahub-fastapi
```txt
.
├── sonar-project.properties
├── app
    ├── pytest.ini
    ├── requirements.txt
    ├── fastapi_celery
        ├── models
        └── template_processors
    └── tests
        ├── ..._test.py
        └── samples
```

2. Install all requirement packages
```bash
# In the app directory where the pytest.ini file is located
cd ./app
python3 -m pip install -r ./requirements.txt

# Run the pytest to update coverage
pytest
```

3. Upload coverage to SonarQube Server
```bash
python3 -m pip install pysonar
set SONAR_HOST_URL=http(s)://<sonarqube-server>:<port>
set SONAR_TOKEN=<sonarqube-server-token>

# run pysonar from the project root directory - dksh-datahub-fastapi
# where the sonar-project.properties file is located
pysonar # -v --verbose
```
