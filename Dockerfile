FROM public.ecr.aws/lambda/python:3.12

# Boto3 is included in the base image, so we don't need a requirements.txt
COPY pattern_inserter.py ./

# The CMD is not strictly necessary as Batch will run the command, but it's good practice.
CMD ["pattern_inserter.py"]