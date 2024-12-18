# define the AWS region
provider "aws" {
  region = "ap-south-1"
}

# S3 Bucket
resource "aws_s3_bucket" "data_storage_bucket" {
  bucket = "rearc-assessment"
}

# S3 Bucket Notification for SQS
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.data_storage_bucket.id

  queue {
    queue_arn     = aws_sqs_queue.json_notification_queue.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".json"
  }
}

# SQS Queue
resource "aws_sqs_queue" "json_notification_queue" {
  name = "json-notification-queue"
}

# Lambda Function 1 (For Part 1 & 2 activities - read data from BLS and Population servers)
resource "aws_lambda_function" "data_processor" {
  function_name = "data_processor_lambda"
  runtime       = "python3.9"
  role          = aws_iam_role.lambda_role.arn
  handler       = "lambda_function.lambda_handler"

    # ZIP file of Part 1&2 Lambda function
  filename         = "data_processor.zip" 
  source_code_hash = filebase64sha256("data_processor.zip")

  environment {
    variables = {
      BUCKET_NAME = aws_s3_bucket.data_storage_bucket.bucket
    }
  }
}

# Lambda Function 2 (Part 3 - to read the data from S3, join and to generate the final report)
resource "aws_lambda_function" "report_generator" {
  function_name = "report_generator_lambda"
  runtime       = "python3.9"
  role          = aws_iam_role.lambda_role.arn
  handler       = "lambda_function.lambda_handler"

    # ZIP file of Part3 Lambda function
  filename         = "report_generator.zip" 
  source_code_hash = filebase64sha256("report_generator.zip")
}

# Defining Lambda Role
resource "aws_iam_role" "lambda_role" {
  name               = "lambda_execution_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action    = "sts:AssumeRole",
        Effect    = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy_attachment" "lambda_policy_attachment" {
  name       = "lambda-policy-attachment"
  roles      = [aws_iam_role.lambda_role.name]
  policy_arn = "arn:aws:iam::aws:policy/AWSLambdaBasicExecutionRole"
}

# SQS Trigger for Lambda Function 2
resource "aws_lambda_event_source_mapping" "sqs_trigger" {
  event_source_arn = aws_sqs_queue.json_notification_queue.arn
  function_name    = aws_lambda_function.report_generator.arn
}

# EventBridge Rule for Lambda Function 1
resource "aws_cloudwatch_event_rule" "daily_trigger" {
  name        = "daily_lambda_trigger"
  schedule_expression = "rate(1 day)"
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.daily_trigger.name
  target_id = "data_processor_lambda"
  arn       = aws_lambda_function.data_processor.arn
}

resource "aws_lambda_permission" "allow_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.data_processor.arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_trigger.arn
}