FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md index_engine.py ./
RUN pip install --no-cache-dir . && useradd --create-home calculator
USER calculator
ENTRYPOINT ["crypto-index"]
CMD ["run"]
