FROM python:3.9-slim

RUN apt-get update &&\
    apt-get install git -y

COPY ingress/requirements.txt /var/local/universalis/

RUN groupadd universalis \
    && useradd -m -d /usr/local/universalis -g universalis universalis

RUN pip3 install --upgrade pip \
    && pip3 install --prefix=/usr/local -r /var/local/universalis/requirements.txt

WORKDIR /usr/local/universalis

COPY --chown=universalis ingress ingress
COPY --chown=universalis coordinator coordinator
COPY --chown=universalis common common

COPY start-ingress.sh /usr/local/bin/
RUN chmod a+x /usr/local/bin/start-ingress.sh

ENV PYTHONPATH /usr/local/universalis

USER universalis
CMD ["/usr/local/bin/start-ingress.sh"]

EXPOSE 8888