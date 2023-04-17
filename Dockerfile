FROM python:3.11.2-buster AS py311
RUN pip install poetry==1.4.2
WORKDIR /usr/src
COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock
RUN poetry install --only dev
ENTRYPOINT poetry run tox
