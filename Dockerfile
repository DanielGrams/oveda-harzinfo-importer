FROM python:3.7-slim-buster

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Environment variables
ENV REDIS_URL=""
ENV API_URL=""
ENV ORGANIZATION_ID=""
ENV CLIENT_ID=""
ENV CLIENT_SECRET=""
ENV ACCESS_TOKEN=""
ENV REFRESH_TOKEN=""

# Install pip requirements
COPY requirements.txt .
RUN python -m pip install -r requirements.txt

WORKDIR /job
COPY . /job

RUN chmod u+x ./entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]
