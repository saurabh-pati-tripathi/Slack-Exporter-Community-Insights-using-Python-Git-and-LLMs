import subprocess
import os
from datetime import datetime, timedelta
import pytz
import boto3
from botocore.exceptions import ClientError
import json
import csv
import anthropic

def get_completion(prompt, model="claude-3-haiku-20240307"):

    api_key = os.environ['CLAUDE_API_KEY']
    claude_client = anthropic.Client(api_key=api_key)

    response = claude_client.messages.create(
        model=model,
        max_tokens=2048,
        system="""You are a Senior product manager having 10 years of experience and working with Atlas\
- a customer support tool offering a seamless experience for both agents and customers.You overlook Content, Product Growth, Sales and Marketing.\

The information about Atlas is here:\
## **All-in-One Customer Support Suite**\

Atlas unifies multiple support tools into a single suite, offering a seamless experience for both agents and customers.\
The platform integrates functionalities like chatbots, session replays, omnichannel communications, and insightful analytics, all under one roof.\
This integration eliminates the need to juggle different software, leading to increased efficiency and a cohesive support strategy.\

## **Features Breakdown**\

1. **Chatbots**: Atlas allows you to create intelligent, automated workflows that guide customers through their support journey. With an intuitive drag-and-drop interface, you can design chatbot interactions that handle common queries, gather information, and direct customers to the right resources, reducing the demand on human agents.\
2. **Custom Data**: Tailor your support system with custom fields and events to collect specific data points relevant to your customers. This feature lets you personalize interactions and better understand customer needs, leading to more targeted support.\
3. **Help Center**: Reduce your support team's workload with a self-service knowledge base. Atlas' Help Center lets customers find answers to their questions, empowering them to resolve issues independently.\
4. **Insights**: Leverage powerful analytics to cut through the noise and identify what matters most to your customers. Atlas provides visual graphs and metrics that help you track performance, understand trends, and make data-driven decisions without needing a data analyst.\
5. **Omnichannel Support**: Communicate with your customers where they are, be it chat, email, SMS, WhatsApp, or Slack. Atlas' unified interface ensures context and continuity across all channels.\
6. **Reports**: Stay informed with customizable reports that track the quality of your customer support. These reports are sent directly to your inbox, allowing you to monitor team performance and customer satisfaction over time.\
7. **Session Replay**: Address issues more effectively with the ability to replay customer sessions. This visual aid provides context to customer problems, helping you diagnose and solve them faster.\
8. **Smart Assist**: AI-powered assistance offers suggested responses, articles, and summaries to enhance the efficiency of your support team. This feature helps you maintain high-quality, consistent communication with customers.\
9. **Timeline**: View a customer's journey chronologically, providing a complete context for every interaction. This helps your team deliver personalized and informed support.\

## **Efficiency and Productivity Tools**\

Atlas emphasizes efficiency with features that cater to a fast-paced work environment:\

- **Inbox**: Manage tickets efficiently with custom inboxes for different customer segments, allowing you to prioritize and organize support issues effectively.\
- **Composer**: Respond quickly with canned responses and easily accessible help articles, streamlining communication and ensuring consistency.\
- **Modern Tooling**: Navigate support issues with AI-enhanced tooling designed to work around your workflows, not the other way around.\
- **Search**: Conduct advanced searches across customer data effortlessly, enabling you to find the information you need rapidly.\
- **Keyboard First**: Utilize keyboard shortcuts to take action and navigate the platform, saving time and increasing productivity.\
- **Command Menu**: Access a command menu for everyday tasks, making it easy to find customers and tickets without manual searching.\

## **Conclusion**\

Atlas offers a comprehensive platform that caters to the needs of modern businesses seeking to optimize their customer support.\
By combining powerful features emphasizing efficiency and user-friendliness,\
Atlas provides founders, CX leaders, and managers with the tools necessary to deliver exceptional customer service.\

Whether you're looking to streamline your support operations, improve customer engagement, or gain deeper insights into your customer service performance,\
Atlas is equipped to meet those challenges. With Atlas, your support team can be empowered to provide a best-in-class customer experience that fosters\
loyalty and drives business success.""",
    messages=[
        {"role": "user", "content": prompt}
    ]
)

    output = response.content[0].text.replace('\n', ' ')
    return json.dumps({"response": output})

def process_individual_file(input_folder_path, output_folder_path):
    # Create the output folder if it doesn't exist
    os.makedirs(output_folder_path, exist_ok=True)

    for file_name in os.listdir(input_folder_path):
        if file_name.endswith('.csv'):
            input_file = os.path.join(input_folder_path, file_name)
            output_file = os.path.join(output_folder_path, file_name)

            with open(input_file, 'r', newline='', encoding='utf-8') as input_file:
                reader = csv.DictReader(input_file)
                data = list(reader)

                # Check if data list is empty
                if data:
                    # Generate the prompt based on your requirements
                    prompt = f"""Generate a report covering the period from {data[0]['Date & Time']} to {data[-1]['Date & Time']} in the {file_name} file.\
Provide an overview of the discussions, questions, and engagements during that time, with the aim of capturing valuable\
insights to support Atlas's goal of leveraging conversations from the Support-Driven community to engage with its members.\
The aim of this report is to capture the key discussions, questions, and engagements in the Channel from {data[0]['Date & Time']} to {data[-1]['Date & Time']},\
aligned with Atlas's goal of leveraging conversations from the Support-Driven community. The report aims to assist in solving important issues, building trust,\
and raising awareness about Atlas by providing valuable insights and recommendations based on the discussions.\

Context: The report covers the {file_name} data Channel from {data[0]['Date & Time']} to {data[-1]['Date & Time']}, focusing on discussions relevant to Atlas's aim of\
leveraging conversations from the Support-Driven community. The report aims to provide insights into the discussions and engagement within the community,\
supporting Atlas's goal of solving important issues, building trust, and raising awareness about the platform. The discussion group comprises founders,\
product managers, CXOs, and other experts with knowledge and expertise in customer experience, technology, and development.\

Instructions for Responding:\
Return the response in JSON format with the following keys:\
{{
"Summary": "Summarize the topics discussed during <Start Date> to <End Date>",
"ReportingPeriod": "From <Start Date> to <End Date>",
"TopQuestions": [
{{
"Question": "Question 1",
"Reactions": "Number of reactions",
"Replies": "Number of replies"
}},
{{
"Question": "Question 2",
"Reactions": "Number of reactions",
"Replies": "Number of replies"
}},
{{
"Question": "Question 3",
"Reactions": "Number of reactions",
"Replies": "Number of replies"
}},
{{
"Question": "Question 4",
"Reactions": "Number of reactions",
"Replies": "Number of replies"
}},
{{
"Question": "Question 5",
"Reactions": "Number of reactions",
"Replies": "Number of replies"
}}
],
"MostEngagingDiscussion": {{
"Discussion": "Title of the most engaging discussion",
"Reactions": "Number of reactions",
"Replies": "Number of replies",
"Participants": ["Participant 1", "Participant 2"]
}},
"ActiveParticipants": [
{{
"Name": "Participant Name",
"Contributions": "Description of contributions",
"Expertise": "Area of expertise"
}}
],
{{"EmergingTrends": "Description of any emerging trends or recurring challenges discussed during the specified period that can inform Atlas's strategy.",
"BlogOpportunities": "Identify any blog article opportunities for Atlas"
"Recommendations": "Potential solutions or recommendations based on the discussions to support Atlas in achieving its aim of engaging with the Support-Driven community."
}}
}}

Do not use coding syntax markers like <```json> around your response.\
Do not add extra commentary except the JSON response.\
Take utmost care in using double quotes in the response.\
Do not change original texts if you are using them in response wherever possible.\

Objective:

1. Summarize the topics discussed during the period and highlight their relevance to Atlas's objectives.\
2. Identify the top 5 questions asked (wrt. number of reactions and replies) and provide the number of reactions and replies received for these question.\
3. Highlight the most engaging discussion (wrt. number of reactions and replies) and the participants involved to identify valuable insights and perspectives.\
4. Identify the active participants during the period and highlight their contributions and expertise.\
5. Identify any emerging trends or recurring challenges discussed that can inform Atlas's strategy and support in solving important issues.\
6. Identify any blog article opportunities for Atlas.\
7. Provide potential solutions or recommendations based on the discussions to support Atlas in achieving its aim of engaging with the Support-Driven community.\

Message:
<{data}>
"""

                    # Get the completion from the ChatGPT API
                    response = get_completion(prompt)
                    print(response)

                    try:
                        # Parse the JSON response
                        report_data = json.loads(json.loads(response)["response"])
                    except json.JSONDecodeError:
                        print(f"Error parsing JSON: {response}")
                        continue

                    # Write the report data to a CSV file
                    fieldnames = list(report_data.keys())
                    with open(output_file, 'w', newline='', encoding='utf-8') as output_file:
                        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerow(report_data)
                else:
                    print(f"No data found in {input_file}. Skipping file.")

def convert_json_to_csv(input_folder, output_folder):

    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Specify the selected folders to process
    selected_folders = ['chit-chat', 'chat-highlights', 'welcome-and-introductions', 'job-board', 'leadership',
                        'career-development', 'customer-experience', 'customer-success', 'knowledge-management',
                        'u-zendesk', 'support-operations', 'events', 'technology', 'about', 'vent',
                        'zlocal-remote', 'metrics-data-kpis', 'good-news', 'quality', 'bulletin-board', 'onboarding',
                        'product-management', 'outsourcing']

    # Specify the field names to extract from the JSON file"
    selected_fields = ["Thread Id","Date & Time","User's Name","Text Message","Thread Date & Time","Message Type","Total Reactions"]

    # Iterate over the selected folders
    for folder_name in selected_folders:
        folder_path = os.path.join(input_folder, folder_name)

        # Check if the selected folder exists
        if os.path.exists(folder_path):
            # Specify the path for the corresponding CSV file
            csv_file = os.path.join(output_folder, folder_name + '.csv')

            # Iterate over the files within the subfolder
            rows = []  # List to store rows before sorting
            for file_name in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file_name)

                # Check if the current item is a JSON file
                if file_name.endswith('.json'):
                    # Open the JSON file and load the data
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                        # Convert each JSON object to a CSV row
                        for obj in data:
                            selected_data = {}
                            # Skip the entire row if subtype is channel join
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

            # Sort the rows based on the "Thread Date & Time" column first, and then by "Date & Time"
            rows.sort(key=lambda x: (datetime.strptime(x.get('Thread Date & Time', ''), '%Y-%m-%d %H:%M:%S') if x.get('Thread Date & Time') else datetime.min,
                                    datetime.strptime(x.get('Date & Time', ''), '%Y-%m-%d %H:%M:%S') if x.get('Date & Time') else datetime.min))

            # Open the CSV file and write headers using specified fields
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=selected_fields)
                writer.writeheader()
                # Write the sorted rows to the CSV file
                writer.writerows(rows)

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
    a_week_ago = datetime.now(pytz.timezone('UTC')) - timedelta(weeks=1)
    formatted_date = a_week_ago.strftime('%Y-%m-%dT%H:%M:%S')
    ist_time = pytz.timezone('Asia/Kolkata')
    timestamp = a_week_ago.astimezone(ist_time).strftime('%Y-%m-%d-%H-%M-%S')
    
    export_filename = f"support_driven_{timestamp}"
    export_dir = f"/tmp/{export_filename}"
    s3_prefix = f"{export_filename}_channels_analyzed_csv/"

    cookie = os.getenv('COOKIE')
    slack_token = os.getenv('SLACK_TOKEN')
    
    command = ['./slackdump', '-cookie', cookie, '-t', slack_token, '-dump-from', formatted_date, '-export', export_dir, '@channels.txt']
    result = subprocess.run(command, text=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    print(result.stdout)
    print(result.stderr)

    # Check if the export directory was created
    if os.path.exists(export_dir):
        print('Slack messages exported to {}'.format(export_dir))

        try:
            input_folder = export_dir
            output_folder = f"/tmp/{export_filename}_extracted_csv"
            convert_json_to_csv(input_folder, output_folder)
            print(os.listdir(output_folder))
            print('CSV files written successfully')
        except ClientError as e:
            print('Error writing CSV files')

        try:
            input_folder_path  = output_folder
            output_folder_path = f"/tmp/{export_filename}_channels_analyzed_csv"
            process_individual_file(input_folder_path, output_folder_path)
            print('Channels files analyzed successfully')
        except ClientError as e:
            print('Error analysing channels files')

        # Create an S3 client
        s3 = boto3.client('s3')

        # Upload the directory to S3
        try:
            upload_dir_to_s3(output_folder_path, 'slackdumpchannelsanalysiscsv', s3, s3_prefix)
            print(f"Uploaded {output_folder_path} to S3 bucket 'slackdumpchannelsanalysiscsv' with prefix '{s3_prefix}'")
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
        'body': 'function executed successfully'
    }

