FROM python:3.10-slim

RUN groupadd universalis \
    && useradd -m -d /usr/local/universalis -g universalis universalis

USER universalis

COPY --chown=universalis:universalis statefun-client/requirements.txt /var/local/universalis/
COPY --chown=universalis:universalis universalis-package /var/local/universalis-package/

ENV PATH="/usr/local/universalis/.local/bin:${PATH}"

RUN pip install --upgrade pip \
    && pip install --user -r /var/local/universalis/requirements.txt \
    && pip install --user ./var/local/universalis-package/

COPY --chown=universalis:universalis stateful_dataflows.tgz /var/local/universalis/
RUN pip install --user /var/local/universalis/stateful_dataflows.tgz
RUN pip install --user pytest

WORKDIR /usr/local/universalis

COPY --chown=universalis:universalis statefun-client .

ENV PYTHONPATH /usr/local/universalis

USER universalis
CMD ["sanic", "app.app", "--host=0.0.0.0", "--port=5000"]

# default port
EXPOSE 5000