FROM python:3.7

RUN apt-get update -y
RUN apt-get install nano jq
RUN pip install nose netCDF4 pymongo xarray

WORKDIR /app
COPY . .
CMD ['python', 'choosefiles.py']
