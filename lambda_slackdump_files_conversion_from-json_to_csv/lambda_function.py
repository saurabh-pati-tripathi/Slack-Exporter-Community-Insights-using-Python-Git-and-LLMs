import boto3
import os
import sys
import uuid
from urllib.parse import unquote_plus
import json
import csv
import pytz
from datetime import datetime

s3_client = boto3.client('s3')

def convert_to_csv(input_file_path, output_file_path):

    selected_fields = ["Thread Id","Date & Time","User's Name","Text Message","Thread Date & Time","Message Type","Total Reactions"]

    rows = []

    if input_file_path.endswith('.json'):
        with open(input_file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)
            for obj in data:
                selected_data = {}
                if obj.get('subtype') == 'channel_join':
                    continue
                for field in selected_fields:
                    if field == 'Date & Time':
                        if 'ts' in obj:
                            timestamp = float(obj['ts'])
                            utc_date = datetime.fromtimestamp(timestamp, pytz.utc)
                            ist_date = utc_date.astimezone(pytz.timezone('Asia/Kolkata'))
                            selected_data[field] = ist_date.strftime('%Y-%m-%d %H:%M:%S')
                    elif field == "User's Name":
                        selected_data[field] = obj.get('user_profile', {}).get('real_name')
                    elif field == 'Text Message':
                            selected_data[field] = obj.get('text')
                    elif field == 'Thread Date & Time':
                        if 'thread_ts' in obj:
                            timestamp = float(obj['thread_ts'])
                            utc_date = datetime.fromtimestamp(timestamp, pytz.utc)
                            ist_date = utc_date.astimezone(pytz.timezone('Asia/Kolkata'))
                            selected_data[field] = ist_date.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            timestamp = float(obj['ts'])
                            utc_date = datetime.fromtimestamp(timestamp, pytz.utc)
                            ist_date = utc_date.astimezone(pytz.timezone('Asia/Kolkata'))
                            selected_data[field] = ist_date.strftime('%Y-%m-%d %H:%M:%S')
                    elif field == 'Thread Id':
                        selected_data[field] = obj.get('ts')
                    elif field == 'Message Type':
                        if 'parent_user_id' not in obj:
                            selected_data[field] = 'Primary Message'
                        else:
                            selected_data[field] = 'Reply'
                    elif field == 'Total Reactions':
                        reactions = obj.get('reactions')
                        reaction_count = sum(reaction['count'] for reaction in reactions) if reactions else 0
                        selected_data[field] = reaction_count
                    else:
                        selected_data[field] = None

                rows.append(selected_data)

    rows.sort(key=lambda x: (datetime.strptime(x.get('Thread Date & Time', ''), '%Y-%m-%d %H:%M:%S') if x.get('Thread Date & Time') else datetime.min,
                             datetime.strptime(x.get('Date & Time', ''), '%Y-%m-%d %H:%M:%S') if x.get('Date & Time') else datetime.min))

    with open(output_file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=selected_fields)
        writer.writeheader()
        writer.writerows(rows)

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        tmpkey = key.replace('/', '')
        download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
        newkey = tmpkey.replace('.json', '')
        upload_path = '/tmp/{}.csv'.format(newkey)

        # Check if the file is one of the excluded files
        if os.path.basename(key) in ['dms.json', 'mpims.json', 'channels.json', 'groups.json', 'users.json']:
            print(f"Skipping {os.path.basename(key)} file.")
            continue
        
        s3_client.download_file(bucket, key, download_path)
        convert_to_csv(download_path, upload_path)
        upload_key = key.replace('.json', '')
        s3_client.upload_file(upload_path, '{}csv'.format(bucket), '{}.csv'.format(upload_key))
