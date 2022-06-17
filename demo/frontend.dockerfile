FROM python:3-slim

COPY demo/requirements.txt /var/local/universalis/
COPY universalis-package /var/local/universalis-package/

RUN groupadd universalis \
    && useradd -m -d /usr/local/universalis -g universalis universalis

RUN pip install --upgrade pip \
    && pip install --prefix=/usr/local -r /var/local/universalis/requirements.txt \
    && pip install --prefix=/usr/local ./var/local/universalis-package/

WORKDIR /usr/local/universalis/demo

COPY --chown=universalis demo .

ENV PYTHONPATH /usr/local/universalis

USER universalis
CMD ["sanic", "app.app", "--host=0.0.0.0", "--port=5000", "--workers=4"]

# default port
EXPOSE 5000