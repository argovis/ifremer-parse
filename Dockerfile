FROM python:3.7

RUN apt-get update -y
RUN apt-get install nano
RUN pip install nose netCDF4 pymongo xarray numpy

WORKDIR /app
COPY . .
CMD ['python', 'choosefiles.py']
