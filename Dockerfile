FROM python:3.7-stretch

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
                       git \
    && rm -rf /var/lib/apt/lists/*

# PostgreSQL client
RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
ENV PG_MAJOR 9.5
RUN echo 'deb http://apt.postgresql.org/pub/repos/apt/ jessie-pgdg main' $PG_MAJOR > /etc/apt/sources.list.d/pgdg.list
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client-$PG_MAJOR \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /code/relations
WORKDIR /code/relations

COPY requirements.txt /code/relations
RUN pip3 install --no-cache-dir -r requirements.txt

CMD /code/relations/bin/_dummy_loop.py
