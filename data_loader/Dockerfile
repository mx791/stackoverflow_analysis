FROM python:alpine3.17

RUN \
    apk add --no-cache postgresql-libs && \
    apk add --no-cache --virtual .build-deps gcc musl-dev postgresql-dev

WORKDIR /app/scraper

COPY requirements.txt /app/scraper/requirements.txt

RUN pip install -r requirements.txt

COPY pythonImport.py /app/scraper/pythonImport.py

CMD [ "python", "pythonImport.py" ]