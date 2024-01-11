FROM quay.io/astronomer/astro-runtime:10.0.0

RUN python -m venv soda_venv && source soda_venv/bin/activate && \
    pip install --no-cache-dir soda-core-snowflake==3.0.47 &&\
    pip install --no-cache-dir soda-core-scientific==3.0.47 && deactivate