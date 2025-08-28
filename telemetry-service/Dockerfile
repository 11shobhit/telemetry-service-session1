# Start from a lightweight official Python image (Debian-based, slim variant)
# - python:3.9-slim-buster means Python 3.9 + Debian Buster (minimal packages)
# - Using slim makes the image much smaller than full Debian
FROM python:3.9-slim-buster

# Set the working directory inside the container
# - All subsequent commands (COPY, RUN, CMD, etc.) will run relative to /app
# - When container starts, it will treat /app as "home" for the service
WORKDIR /app

# Copy the requirements.txt file into the container
# - Only requirements.txt is copied here (not the whole codebase yet)
# - This allows Docker to leverage build cache: dependencies are installed only
#   when requirements.txt changes (not when your code changes)
COPY requirements.txt .

# Install Python dependencies from requirements.txt
# - --no-cache-dir prevents pip from saving download caches, making the image smaller
# - This step installs paho-mqtt==1.6.1 (from your requirements.txt)
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application source code into the container at /app
# - Includes your main.py, modules, configs, etc.
COPY . .

# Define the default command to run when the container starts
# - Here it runs Python and executes main.py
# - This file should contain the `if __name__ == "__main__": run()` block
CMD ["python", "main.py"]
