FROM python:3.9-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends git iputils-ping && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-postgres==1.8.*

RUN useradd -m dbtuser

USER dbtuser

WORKDIR /dbt

COPY --chown=dbtuser:dbtuser . /dbt/

RUN dbt deps --profiles-dir /dbt

ENTRYPOINT ["dbt"]
CMD ["run", "--profiles-dir", "/dbt"]
