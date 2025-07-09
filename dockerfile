# Base image  
FROM python:3.13.5-bookworm

# Work directory 
WORKDIR /app


# Copy the current directory contents into the container at /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dump_into_csv.py .

CMD ["python", "dump_into_csv.py"]