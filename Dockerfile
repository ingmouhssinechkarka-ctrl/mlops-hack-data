# 1. Start from an image that already has Python
FROM python:3.12-slim-bookworm

# 2. Install Java (Spark needs it)
RUN apt-get update && apt-get install -y openjdk-17-jre-headless

# 3. Work inside the /app folder
WORKDIR /app

# 4. Install the Python libraries
COPY requirements.txt .
RUN pip install -r requirements.txt

# 5. Copy the project code and data
COPY pipeline/ pipeline/
COPY data/ data/
COPY run_pipeline.py register_best_model.py predict_with_approved.py ./

# 6. Command run when the container starts
CMD ["python", "run_pipeline.py"]
