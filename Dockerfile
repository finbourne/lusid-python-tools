FROM python:3.7.0

WORKDIR /usr/src/

RUN apt-get update && apt-get -y install jq

COPY requirements.txt /usr/src/
RUN pip install --no-cache-dir -r requirements.txt

RUN pip --no-cache-dir install --upgrade awscli

ENTRYPOINT PYTHONPATH=/usr/src/:/usr/src/tests python -m unittest discover -v