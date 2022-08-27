FROM public.ecr.aws/lambda/python:3.9
RUN pip install prefect
ADD ml.py .
CMD ["ml.handler"]