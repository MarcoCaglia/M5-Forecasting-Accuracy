# Import images
FROM alpine
FROM python:3.8

# Create WorkDir
WORKDIR /home/usr/M5-Forecasting-Accuracy

# Copy Files
COPY ./config ./config
COPY ./local/m5-forecasting-accuracy.zip m5-forecasting-accuracy.zip
COPY ./src ./src
COPY requirements.txt requirements.txt

# Install dependencies
RUN pip3 install -r requirements.txt

# Run executor
CMD python3 ./src/executor.py