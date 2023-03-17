FROM python:3.11.2

WORKDIR /usr/src/

RUN apt-get update && apt-get -y install jq

# cache requirements as pip install can take long
# and requirements shouldn't change often
COPY requirements.txt /usr/src/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . /usr/src/
ENTRYPOINT PYTHONPATH=/usr/src/:/usr/src/tests python -m unittest discover -v