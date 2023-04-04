FROM python:3.11.2-buster as py311
RUN pip install poetry==1.4.2
COPY . .
RUN poetry install --only dev
ENTRYPOINT poetry run tox
