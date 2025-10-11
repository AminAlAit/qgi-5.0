FROM python:3.12-slim

# Copy the requirements file and install dependencies first
COPY requirements.txt .
RUN pip install -r requirements.txt

# Now copy the application script
COPY src/pattern_inserter.py .

# Set the command to run the script
CMD ["python", "pattern_inserter.py"]