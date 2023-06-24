FROM apache/superset:2.1.0

USER root

ENV CHROME_VERSION=102.0.5005.61-1
RUN apt-get update && \
    apt-get install --no-install-recommends -y wget tar unzip && \
    wget -q https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_${CHROME_VERSION}_amd64.deb && \
    apt-get install -y --no-install-recommends ./google-chrome-stable_${CHROME_VERSION}_amd64.deb && \
    rm -f google-chrome-stable_current_amd64.deb

ENV CHROMEDRIVER_VERSION=102.0.5005.27
RUN wget -q https://chromedriver.storage.googleapis.com/${CHROMEDRIVER_VERSION}/chromedriver_linux64.zip && \
    unzip chromedriver_linux64.zip -d /usr/bin && \
    chmod 755 /usr/bin/chromedriver && \
    rm -f chromedriver_linux64.zip

RUN pip install --no-cache gevent psycopg2 redis

USER superset