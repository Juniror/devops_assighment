FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy source code
COPY src/calculator.py /app/
COPY src/app.py /app/

# Run the application
CMD ["python", "app.py"]