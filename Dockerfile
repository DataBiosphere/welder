FROM google/cloud-sdk:slim

RUN apt-get update -y && \
    apt-get install -y python-pip python-dev

RUN mkdir /data

ENV WELDERVOLUME=/data
# We copy just the requirements.txt first to leverage Docker cache
COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip install -r requirements.txt

COPY . /app

ENTRYPOINT [ "python" ]

CMD [ "welder.py" ]
