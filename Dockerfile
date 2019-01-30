FROM python:3.7-alpine
COPY . /tmp/build/
RUN pip install --cache-dir=/tmp/cache /tmp/build && rm -rf /tmp/cache /tmp/build

EXPOSE 8000

ENTRYPOINT ["/usr/local/bin/queueueue"]
CMD ["--help"]

