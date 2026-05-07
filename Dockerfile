# Use an official lightweight Python image
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
# (We install boto3, pandas, scikit-learn, joblib)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your consumer script into the container
COPY src/app/consumer.py /app/consumer.py

# Run the consumer script when the container starts
CMD ["python", "/app/consumer.py"]
