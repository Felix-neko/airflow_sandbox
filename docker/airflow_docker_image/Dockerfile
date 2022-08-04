FROM apache/airflow:2.3.3-python3.7
USER root
RUN echo 'deb http://security.debian.org/debian-security stretch/updates main' >> /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y software-properties-common openjdk-8-jdk && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip3 install --no-cache-dir --extra-index-url https://binary.alfabank.ru/artifactory/api/pypi/fs_etl-pypi/simple fs-etl==0.6.7
