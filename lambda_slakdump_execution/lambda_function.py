import subprocess
import os
from datetime import datetime, timedelta
import pytz
import boto3
from botocore.exceptions import ClientError

def upload_dir_to_s3(local_dir, bucket_name, s3_client, s3_prefix):
    """
    Recursively uploads a local directory to an S3 bucket with a specified prefix.
    :param local_dir: The local directory to upload.
    :param bucket_name: The name of the S3 bucket.
    :param s3_client: The Boto3 S3 client.
    :param s3_prefix: The prefix to use as a virtual directory in the S3 bucket.
    """
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            local_file_path = os.path.join(root, file)
            s3_file_path = os.path.join(s3_prefix, os.path.relpath(local_file_path, local_dir))
            try:
                s3_client.upload_fileobj(open(local_file_path, 'rb'), bucket_name, s3_file_path)
            except ClientError as e:
                print(f"Error uploading file {local_file_path} to S3: {e}")

def lambda_handler(event, context):
    # Calculate time and construct the file name
    two_hours_ago = datetime.now(pytz.timezone('UTC')) - timedelta(hours=2)
    formatted_date = two_hours_ago.strftime('%Y-%m-%dT%H:%M:%S')
    ist_time = pytz.timezone('Asia/Kolkata')
    timestamp = two_hours_ago.astimezone(ist_time).strftime('%Y-%m-%d-%H-%M-%S')
    
    export_filename = f"support_driven_{timestamp}"  # Ensure the extension matches expected output
    export_dir = f"/tmp/{export_filename}"
    s3_prefix = f"support_driven_{timestamp}/"

    cookie = os.getenv('COOKIE')
    slack_token = os.getenv('SLACK_TOKEN')
    
    command = ['./slackdump', '-cookie', cookie, '-t', slack_token, '-dump-from', formatted_date, '-export', export_dir, '@channels.txt']
    result = subprocess.run(command, text=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    print(result.stdout)
    print(result.stderr)

    # Check if the export directory was created
    if os.path.exists(export_dir):
        print('Slack messages exported to {}'.format(export_dir))

        # Create an S3 client
        s3 = boto3.client('s3')

        # Upload the directory to S3
        try:
            upload_dir_to_s3(export_dir, 'slackdumpfiles', s3, s3_prefix)
            print(f"Uploaded {export_dir} to S3 bucket 'slackdumpfiles' with prefix '{s3_prefix}'")
        except ClientError as e:
            print(f"Error uploading directory to S3: {e}")
            return {
                'statusCode': 500,
                'body': 'Error uploading directory to S3'
            }
    else:
        print('Error: Slack messages were not exported')

    return {
        'statusCode': 200,
        'body': 'slackdump executed successfully'
    }
