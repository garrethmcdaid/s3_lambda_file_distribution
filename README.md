# s3_lambda_file_distribution
Python script that can be use as AWS Lambda trigger to automatically move files between S3 buckets

This script can be used as a AWS Lambda function that is triggered upon the creation of a new S3 object so that the object is moved to another S3 location if it meets certain criteria.

The criteria against which to test eligibility of new objects for moving are set in a JSON object that is downloaded from a remote HTTP source.

Multiple match criteria can be included in the JSON object, and each new object is evaluated against all criteria.

An example maps.json file is included for demonstration purposes.

An event.json file is also included to allow for testing outside the AWS Lambda and S3 environments.

The script deliberately includes verbose output, to allow for meaningful debug and audit in the AWS Cloudwatch logs that are generated at each function invocation.