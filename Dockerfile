FROM python:3.6.1
WORKDIR /code
COPY python/talend/labs/beam/ml/expansion_service.py expansion_service.py
COPY python/requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
EXPOSE 9097
CMD ["python", "./expansion_service.py", "-p", "9097"]