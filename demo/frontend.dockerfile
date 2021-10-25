FROM python:3-slim

COPY demo/requirements.txt /var/local/universalis/
COPY universalis-package /var/local/universalis-package/

RUN groupadd universalis \
    && useradd -m -d /usr/local/universalis -g universalis universalis

RUN pip install --upgrade pip \
    && pip install --prefix=/usr/local -r /var/local/universalis/requirements.txt \
    && pip install --prefix=/usr/local ./var/local/universalis-package/

COPY --chown=universalis demo demo

WORKDIR /usr/local/universalis/demo

ENV PYTHONPATH /usr/local/universalis

USER universalis
CMD ["gunicorn", "-b", "0.0.0.0:5000", "app:app", "--timeout", "10", "-w", "1"]

# default flask port
EXPOSE 5000