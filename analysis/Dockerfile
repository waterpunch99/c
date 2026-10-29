FROM python:3.12.12-slim-bookworm
WORKDIR /workspace
COPY requirements.txt /tmp/analysis-requirements.txt
RUN pip install --no-cache-dir --only-binary=:all: -r /tmp/analysis-requirements.txt
ENV MPLBACKEND=Agg MPLCONFIGDIR=/tmp/matplotlib
ENTRYPOINT ["python", "scripts/analyze_results.py"]
