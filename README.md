# Recall REST API

REST API example that uses Flask, Kafka, and PostgreSQL.

## Installation

Python >= 3.9

Install with pip:

```
pip install -r requirements.txt
```


## Run

1. Run required dockers:
```
docker run -p 5432:5432 -p 9092:9092 -d public.ecr.aws/b9o6d1l3/exercises:backend
```

2. Run Flask app:
```
python app/app.py
```

3. Goto http://localhost:8080/
