# --- STAGE 1: Builder ---
# This stage installs dependencies into a clean environment.
FROM python:3.9-slim AS builder

# Set the working directory
WORKDIR /app

# Copy only the requirements file to leverage Docker's layer caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt


# --- STAGE 2: Final Image ---
# This stage builds the lean, final image for production.
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the installed packages from the builder stage
COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages

# Copy the source code
COPY src/ ./src/

# Define the command to run the application
CMD ["python", "src/shuffle_job.py"]