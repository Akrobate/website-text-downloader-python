FROM continuumio/miniconda:latest

RUN conda update conda
RUN conda update --all

COPY ./docker-configuration/conda/environment.yml /usr/app/
WORKDIR /usr/app

RUN conda env create -f environment.yml

RUN echo "source activate WebsiteTextDownloader" > ~/.bashrc
ENV PATH /opt/conda/envs/WebsiteTextDownloader/bin:$PATH

COPY ./start-websites-text-downloading.py /usr/app/
