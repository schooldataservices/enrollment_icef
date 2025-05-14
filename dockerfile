# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables (optional, if needed)
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/icef-437920.json

# Expose any necessary ports (if applicable)
# EXPOSE 8080

# Define the command to run the application
CMD ["python", "main.py"]