FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

WORKDIR /template

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/main.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"

RUN java -version
