#FROM anuvaadio/anuvaad-corpus-python-base:3
FROM python:3.6-slim
COPY / /app
WORKDIR /app
RUN pip3 install -r requirements.txt
RUN python -c "import nltk; nltk.download('punkt')"
COPY start.sh /usr/bin/start.sh
RUN chmod +x /usr/bin/start.sh
CMD ["/usr/bin/start.sh"]
