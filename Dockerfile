FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PORT=8080
WORKDIR /app
COPY requirements.txt requirements-cloud.txt ./
RUN pip install --no-cache-dir -r requirements-cloud.txt && useradd --uid 10001 --create-home sourcework
COPY source_agent ./source_agent
COPY examples/default/SourceOnboarding.scala ./examples/default/SourceOnboarding.scala
USER 10001
EXPOSE 8080
CMD ["sh", "-c", "exec uvicorn source_agent.app:app --host 0.0.0.0 --port ${PORT:-8080} --proxy-headers"]
