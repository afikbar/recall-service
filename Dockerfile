FROM python:3.9-slim-buster

WORKDIR /flask-app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

ENTRYPOINT [ "python" ]
CMD [ "app/app.py" ]
