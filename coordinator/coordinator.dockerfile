FROM python:3.9-slim

COPY coordinator/requirements.txt /var/local/universalis/
COPY universalis-package /var/local/universalis-package/

RUN groupadd universalis \
    && useradd -m -d /usr/local/universalis -g universalis universalis

RUN pip install --upgrade pip \
    && pip install --prefix=/usr/local -r /var/local/universalis/requirements.txt \
    && pip install --prefix=/usr/local ./var/local/universalis-package/

WORKDIR /usr/local/universalis

COPY --chown=universalis coordinator coordinator

COPY coordinator/start-coordinator.sh /usr/local/bin/
RUN chmod a+x /usr/local/bin/start-coordinator.sh

ENV PYTHONPATH /usr/local/universalis

USER universalis
CMD ["/usr/local/bin/start-coordinator.sh"]

EXPOSE 8888