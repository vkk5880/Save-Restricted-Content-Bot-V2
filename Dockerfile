# Use a newer Debian base image (Bookworm) which has GLIBC 2.36
# This should resolve the GLIBC_2.36' and 'GLIBC_2.38' not found errors.
FROM python:3.10-slim-bookworm

# Update package lists and upgrade installed packages
# This ensures you have the latest versions of packages available from the Bookworm repositories.
RUN apt update && apt upgrade -y

# Install necessary system dependencies, including ffmpeg.
# These are the tools required for your application, including git, curl, python3-pip, and ffmpeg.
RUN apt-get install -y git curl python3-pip ffmpeg wget bash neofetch software-properties-common

# Copy your application's Python dependency list into the container
COPY requirements.txt .

# Install wheel, which is often a build dependency for other Python packages
RUN pip3 install wheel

# Install Python dependencies from requirements.txt
# --no-cache-dir reduces the image size by not storing pip's cache.
# -U ensures packages are upgraded if they already exist.
RUN pip3 install --no-cache-dir -U -r requirements.txt

# Set the working directory inside the container
WORKDIR /app

# Copy your application code into the container
COPY . .

# Expose port 5000, which your Flask application will use
EXPOSE 5000

# Define the command to run when the container starts
# This will start your Flask application in the background and then run your main Python script.
CMD flask run -h 0.0.0.0 -p 5000 & python3 -m devgagan
