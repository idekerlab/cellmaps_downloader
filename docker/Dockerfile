# Use an official Python runtime as a parent image
FROM continuumio/miniconda3

RUN apt-get --allow-releaseinfo-change update
RUN apt-get install -y build-essential 

RUN mkdir /tmp/cellmaps_downloader
COPY ./ /tmp/cellmaps_downloader/
RUN pip install /tmp/cellmaps_downloader

RUN rm -rf /tmp/cellmaps_downloader

ENTRYPOINT ["/opt/conda/bin/cellmaps_downloadercmd.py"]

CMD ["--help"]
