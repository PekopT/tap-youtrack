FROM --platform=linux/amd64 python:3.9-buster
WORKDIR /app
ADD . .
RUN apt-get update && apt-get install -y libpq-dev python3-dev
RUN pip install .
RUN pip install git+https://github.com/datamill-co/target-postgres.git

ENTRYPOINT tap-youtrack