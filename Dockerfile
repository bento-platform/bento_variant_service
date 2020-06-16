FROM python:3.8-slim-buster

RUN apt-get update -y && apt-get install -y build-essential

RUN python3 -m pip install gunicorn

COPY . /app
WORKDIR /app

RUN mkdir /data
ENV DATA /data

RUN python3 -m pip install . && apt-get purge -y build-essential

# TODO: Right now, running the service with more than one worker could cause
# TODO: strange behaviour.
EXPOSE 8080
ENTRYPOINT ["gunicorn", "bento_variant_service.app:application", "--bind", "0.0.0.0:8080"]
CMD ["--workers", "1"]
