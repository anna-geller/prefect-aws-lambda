FROM public.ecr.aws/lambda/python:3.9
RUN pip install prefect
ADD etl.py .
CMD ["etl.handler"]