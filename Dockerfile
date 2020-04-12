FROM python:3.7-alpine
WORKDIR /code
ENV MY_NODE=$NODE
ENV MY_PORT=$PORT
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
CMD flask run --host='0.0.0.0' --port=$PORT