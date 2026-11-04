@echo off
cd /d "%~dp0"
if not exist .venv (
    echo Creation de l'environnement Python...
    python -m venv .venv
    call .venv\Scripts\activate
    pip install -r requirements.txt
) else (
    call .venv\Scripts\activate
)
set PYSPARK_PYTHON=python
python run_pipeline.py %*
echo.
echo Ouverture de MLflow sur http://127.0.0.1:5000  (Ctrl+C pour arreter)
start "" http://127.0.0.1:5000
mlflow ui
