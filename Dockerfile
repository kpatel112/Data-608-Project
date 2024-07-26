# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port the app runs on
EXPOSE 8050

# Define environment variables for AWS if needed
ENV AWS_DEFAULT_REGION=us-west-2

# Run the application
CMD ["python", "dashboard.py"]
