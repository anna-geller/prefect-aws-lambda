FROM public.ecr.aws/lambda/python:3.9
RUN pip install prefect
ADD hello.py .
CMD ["hello.handler"]