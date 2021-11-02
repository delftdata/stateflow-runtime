FROM python:3.9-slim

RUN apt-get update && apt-get install -y redis-server

COPY worker/redis.conf /usr/local/etc/redis/redis.conf

COPY worker/requirements.txt /var/local/universalis/
COPY universalis-package /var/local/universalis-package/

RUN groupadd universalis \
    && useradd -m -d /usr/local/universalis -g universalis universalis

RUN pip install --upgrade pip \
    && pip install --prefix=/usr/local -r /var/local/universalis/requirements.txt \
    && pip install --prefix=/usr/local ./var/local/universalis-package/

WORKDIR /usr/local/universalis

COPY --chown=universalis worker worker

COPY worker/start-worker.sh /usr/local/bin/
RUN chmod a+x /usr/local/bin/start-worker.sh

ENV PYTHONPATH /usr/local/universalis

USER universalis
CMD ["/usr/local/bin/start-worker.sh"]

EXPOSE 8888