FROM gcr.io/dataflow-templates-base/python3-template-launcher-base
WORKDIR /template

RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*

# Install all Python dependencies into the Flex Template container at image-build time.
# Do NOT set FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE: in a private runtime it makes
# the launcher stage requirements again with "pip download", which requires PyPI access.
COPY requirements.txt /template/requirements.txt
RUN pip install --no-cache-dir -r /template/requirements.txt

COPY main.py /template/main.py

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/main.py"

RUN java -version
