# Docker Push Commands

0. `aws configure --profile qgi-admin-keys` (make sure this profile exists in "/workspace/.aws/config)
1. Don't use: `aws ecr get-login-password --region eu-central-1 | docker login --username AWS --password-stdin <AWS_ACCOUNT_ID>.dkr.ecr.eu-central-1.amazonaws.com`
  a. Instead use: `aws ecr get-login-password --region eu-central-1 --profile qgi-admin-keys | docker login --username AWS --password-stdin <AWS_ACCOUNT_ID>.dkr.ecr.eu-central-1.amazonaws.com`
2. `docker build -t qgi-pipeline-b-scdi-finder .`
3. `docker tag qgi-pipeline-b-scdi-finder:latest <AWS_ACCOUNT_ID>.dkr.ecr.eu-central-1.amazonaws.com/qgi-pipeline-b-scdi-finder:latest`
4. `docker push <AWS_ACCOUNT_ID>.dkr.ecr.eu-central-1.amazonaws.com/qgi-pipeline-b-scdi-finder:latest`
