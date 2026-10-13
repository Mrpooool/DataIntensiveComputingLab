FROM eclipse-temurin:21-jre-jammy AS jre
FROM python:3.11.9-slim-bookworm

COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYTHONPATH=/workspace/src
WORKDIR /workspace

COPY requirements-wikipulse.txt requirements.txt ./
RUN pip install --no-cache-dir -r requirements-wikipulse.txt
COPY src ./src
COPY scripts ./scripts
