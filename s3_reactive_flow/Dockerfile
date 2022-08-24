FROM public.ecr.aws/lambda/python:3.9
RUN pip install prefect awswrangler
ADD s3_reactive_flow.py .
CMD ["s3_reactive_flow.handler"]