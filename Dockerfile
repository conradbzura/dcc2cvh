FROM python:3.12
WORKDIR /data
RUN pip install --no-cache-dir git+https://github.com/conradbzura/dcc2cvh.git
RUN pip install uvicorn
RUN useradd app
USER app
EXPOSE 8000
CMD ["uvicorn", "dcc2cvh.cvh.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
