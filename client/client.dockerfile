FROM python:3.10-slim

RUN groupadd benchmark \
    && useradd -m -d /usr/local/benchmark -g benchmark benchmark

USER benchmark

COPY --chown=benchmark:benchmark client/requirements.txt /var/local/client/
COPY --chown=benchmark:benchmark universalis-package /var/local/universalis-package/

ENV PATH="/usr/local/benchmark/.local/bin:${PATH}"

RUN pip install --upgrade pip \
    && pip install --user -r /var/local/client/requirements.txt \
    && pip install --user ./var/local/universalis-package/

COPY --chown=benchmark:benchmark client /usr/local/benchmark/client

WORKDIR /usr/local/benchmark/client

ENV PYTHONPATH /usr/local/benchmark
USER benchmark