FROM public.ecr.aws/lambda/python:3.9
RUN pip install prefect
ADD healthcheck.py .
CMD ["healthcheck.handler"]