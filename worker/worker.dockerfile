FROM python:3.9-slim

RUN apt-get update &&\
    apt-get install git -y

COPY worker/requirements.txt /var/local/universalis/

RUN groupadd universalis \
    && useradd -m -d /usr/local/universalis -g universalis universalis

RUN pip3 install --upgrade pip \
    && pip3 install --prefix=/usr/local -r /var/local/universalis/requirements.txt

WORKDIR /usr/local/universalis

COPY --chown=universalis worker worker
COPY --chown=universalis common common

COPY worker/start-worker.sh /usr/local/bin/
RUN chmod a+x /usr/local/bin/start-worker.sh

ENV PYTHONPATH /usr/local/universalis

USER universalis
CMD ["/usr/local/bin/start-worker.sh"]

EXPOSE 8888