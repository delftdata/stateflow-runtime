FROM python:3.9-slim

COPY requirements.txt /var/local/universalis/

RUN groupadd universalis \
    && useradd -m -d /usr/local/universalis -g universalis universalis

RUN pip3 install --upgrade pip \
    && pip3 install --prefix=/usr/local -r /var/local/universalis/requirements.txt

WORKDIR /usr/local/universalis

COPY --chown=universalis networking-service networking-service
COPY --chown=universalis universalis_operator universalis_operator

COPY docker-command.sh /usr/local/bin/
RUN chmod a+x /usr/local/bin/docker-command.sh

ENV PYTHONPATH /usr/local/universalis

USER universalis
CMD ["/usr/local/bin/docker-command.sh"]

EXPOSE 8888