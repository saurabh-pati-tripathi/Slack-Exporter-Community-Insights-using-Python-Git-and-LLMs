import boto3
import os
import sys
import uuid
from urllib.parse import unquote_plus
import json
import csv
import pytz
from datetime import datetime, timedelta
import google.auth
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
import anthropic

s3_client = boto3.client('s3')

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CREDENTIALS_FILE = 'client_secret.json'

# Dictionary mapping month names to spreadsheet IDs
SPREADSHEET_IDS = {
    'April': '1NiTmM6Ke1XrW9gDHJT4Cx5UF5epQp2Q8jNOP0VJkqaw',
    'May': '1HfgZfRQoM6X_WRpvnwI2eMSNCTPG7EbBDKK_2NK2HDQ',
    'June': '1Ojc56MA_n9i8bVvbZ9TubkmLMJA71GLAKTn6MC7BBW4',
    'July': '1cyPL0CV84WQM_YnLM91Ngk_JwQG0iOdmBqHeEWktqAM',
    'August': '1-h8Lnm28GzLB0A4VBLIgGglor0ycC7BGrOJYX88oTqQ',
    'September': '1VqbbxrX4hs_Qx8Bwe-sOAk6NaZ-1dMpQr2PZGOIlKJA',
    'October': '1EVKBnFaSDZmp-pDtvvE_V-haAjpn-Rr398AhFIoapNg',
    'November': '12K5KNHG2MIm45ICW2UnMT0XJapNT_FHUqxSHMnIF9HY',
    'December': '18aStweOJGCggpN33I0B-TtSNbuuX7fGurvzy1MVsu4A'
}

def get_spreadsheet_id(sheet_name):
    # Get the current date in Indian timezone
    tz = pytz.timezone('Asia/Kolkata')
    now = datetime.now(tz)

    # Check if it's been more than an hour since the start of a new month
    if now.hour >= 1 and now.minute >= 0 and now.second >= 0:
        month_name = now.strftime('%B')
    else:
        # Get the previous month's name
        prev_month = now - timedelta(days=1)
        month_name = prev_month.strftime('%B')

    # Check if the spreadsheet ID is available for the current/previous month
    if month_name in SPREADSHEET_IDS:
        return SPREADSHEET_IDS[month_name]
    else:
        print(f"No spreadsheet ID found for {month_name}.")
        return None

def append_data_to_sheet(spreadsheet_id, csv_file, sheet_name):
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    else:
        flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
        creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # Check if the sheet exists, if not, create it
        try:
            result = sheet.values().get(spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A1").execute()
            values = result.get('values', [])
        except HttpError as error:
            if error.resp.status == 400 or error.resp.status == 404 :
                # Sheet doesn't exist, create it
                request = {"addSheet": {"properties": {"title": sheet_name}}}
                response = sheet.batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": [request]}).execute()
                sheet_id = response["replies"][0]["addSheet"]["properties"]["sheetId"]
                values = []
            else:
                raise error

        with open(csv_file, 'r') as file:
            csv_reader = csv.reader(file)
            data = list(csv_reader)

        # Check if the sheet is empty
        if not values:
            # Append the header row
            range_name = f"{sheet_name}!A1"
            request = sheet.values().append(spreadsheetId=spreadsheet_id, range=range_name, valueInputOption='USER_ENTERED', body={'values': [data[0]]}).execute()
            print(f"Appended header row to {spreadsheet_id}, sheet {sheet_name}")
        data = data[1:]  # Remove the header row from the data    

        # Find the last row with data
        last_row = len(values) + 1

        # Append the data to the sheet starting from the next row
        range_name = f"{sheet_name}!A{last_row}"
        request = sheet.values().append(spreadsheetId=spreadsheet_id, range=range_name, valueInputOption='USER_ENTERED', body={'values': data}).execute()
        print(f"{len(data)} rows appended to {spreadsheet_id}, sheet {sheet_name}")

    except HttpError as error:
        print(f'An error occurred: {error}')

def get_sheet_name_from_key(key):
    parts = key.split('/')
    if len(parts) >= 2:
        return parts[1]
    else:
        return os.path.splitext(os.path.basename(key))[0]

def get_completion(prompt, model="claude-3-haiku-20240307"):

    api_key = os.environ['CLAUDE_API_KEY']
    claude_client = anthropic.Client(api_key=api_key)

    response = claude_client.messages.create(
        model=model,
        max_tokens=1024,
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

def process_csv(input_file_path, output_file_path):

    parsed_data_list = []

    if input_file_path.endswith('.csv'):

        with open(input_file_path, 'r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)

            for row in reader:
                if row['Message Type'] == 'Primary Message':
                    date_time = row['Date & Time']
                    user_message = row['Text Message']
                    thread_date = row['Thread Date & Time']
                    thread_id = row['Thread Id']

                    prompt = f"""Quick Overview: We've unofficially collected messages from the Support-Driven Slack community, a forum dedicated to customer support topics. This collection includes both original inquiries and their subsequent responses. Please note that some replies might be tagged as original messages, as members sometimes respond directly in the main channel rather than using the dedicated reply feature.\

Objective: Our aim is to leverage these conversations from the Support-Driven community to engage with its members. Through our participation, we hope to assist in solving important issues, build trust, and raise awareness about Atlas. Our discussion group will comprise founders (with expertise in customer experience and technology/development), product managers, CXOs, among others.\

**Instructions for Responding:**\

1. Message Analysis\
Begin by immersing yourself in the primary message, thoroughly understanding its subject matter. Deconstruct and distill this message to its core essence.\
2. Alignment Check\
Evaluate if the message's content is closely aligned with Atlas' core business, industry relevance, or pertinent to the expertise of our founders, PMs, developers, and marketers.\
3. Decision Criteria\
Utilize the defined categories to pinpoint the most appropriate response. Ensure all responses adhere strictly to the choices provided without digressing.\

a. Message Nature: Determine whether it's a "Primary message," "Reply to primary message," or reflects "No context in the message."\

b. Atlas Engagement: Ascertain the importance and suitability of Atlas entering the conversation, with a clear "Yes" or "No."\

c. Engagement Rationale: Offer a concise rationale for Atlas' prospective engagement in the conversation.\

d. Team Contribution: Decide if an Atlas team member (e.g., Product Manager, Founder) should contribute, selecting "Yes" or "No."\

e. Appropriate Participant: Identify the best-suited individual for this dialogue—be it "Product Manager," "Founders," "CXOs," "No one," or "Someone else (please specify)."\

f. Message Classification: Tag the message type, choosing from options like "Introduction," "Feedback," "Problem," "None of these," etc.\

g. Question Type: If a question is present, classify it succinctly according to the provided categories or specify otherwise.\

h. Relevance Score: Assess the message's relevance to Atlas on a 0 to 10 scale, where 0 implies no relevance and 10 indicates high relevance.\

i. Summary: Craft a concise one-sentence summary of the message's core content.\

j. As a member of Atlas, generate a response that the suitable member can respond to. Make the response more humanized and avoid mentioning Atlas or the suitable member unless necessary. Add more detail, and make it an icebreaker to initiate conversation if possible.\

1. Response Format\
Conclude by presenting your analysis in a JSON format, incorporating the following keys:\

Nature of Message, Suitable for Atlas, Reason, Suitable for Team Member, Suitable Member, Classification, Question About, Relevance Score, Summary, Suggested Response

An example of response:\
{{
"Nature of Message": "Primary message",
"Suitable for Atlas": "No",
"Reason": "This message is related to a violation of the community's Code of Conduct, which is not directly related to Atlas' products or services.",
"Suitable for Team Member": "No",
"Suitable Member": "No one",
"Classification": "Policy Violation",
"Question About": "None",
"Relevance Score": 2,
"Summary": "A message about a violation of the community's Code of Conduct and a request for the recipient to acknowledge and stop the violation within 48 hours.",
"Suggested Response": "I understand this is a sensitive issue regarding a violation of the community's code of conduct. As an external party, I don't have the appropriate context to engage directly. However, I would suggest reaching out to the community moderators to resolve this matter in a constructive manner that aligns with the established policies. My role is to provide helpful information about our products, so I'll refrain from further involvement here. Please let me know if there are any other ways I can assist you."
}}

Do not use coding syntax markers like <```json> around your response.\
Do not add extra commentary except the JSON response.\
Take utmost care in using double quotes in the response.\

Message:
<{user_message}>"""

                    response = get_completion(prompt)
                    print(response)

                    try:
                        parsed_data = json.loads(json.loads(response)["response"])
                    except json.JSONDecodeError:
                        print(f"Error parsing JSON: {response}")
                        continue
                    
                    field_mapping = {
                        "Nature of Message": "Nature of Message",
                        "Suitable for Atlas": "Should Atlas participate?",
                        "Reason": "Why?",
                        "Suitable for Team Member": "Suitable for Team Member?",
                        "Suitable Member": "Who should Reply?",
                        "Classification": "Message Classification",
                        "Question About": "Question Type(If Applicable)",
                        "Relevance Score": "Relevance Score",
                        "Summary": "Summary",
                        "Suggested Response": "Suggested Response",  
                        }

                    new_parsed_data = {field_mapping[key]: value for key, value in parsed_data.items() if key in field_mapping}

                    new_parsed_data['Date & Time'] = date_time
                    new_parsed_data['Thread Id'] = thread_id
                    new_parsed_data['Message'] = user_message
                    
                    parsed_data_list.append(new_parsed_data)

    with open(output_file_path, 'w', newline='', encoding='utf-8') as csv_file:
        fieldnames = [
                "Thread Id",
                "Date & Time",                
                "Message",
                "Nature of Message",
                "Should Atlas participate?",
                "Why?",
                "Suitable for Team Member?",
                "Who should Reply?",
                "Message Classification",
                "Question Type(If Applicable)",
                "Relevance Score",
                "Summary",
                "Suggested Response"
            ]

        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(parsed_data_list)

def generate_summary(thread_messages):
    thread_text = '\n'.join(message['Text Message'] for message in thread_messages)
    prompt = f"""Quick Overview: We've informally compiled messages from the Support-Driven Slack community, a platform focusing on customer support. This dataset features inquiries and their replies. Note that due to direct replies in the main channel, some responses might be confused as initial inquiries.\

Objective: We seek to immerse ourselves in the Support-Driven community discussions, providing solutions to pressing issues, fostering trust, and elevating Atlas' visibility. This initiative targets founders with customer experience and tech development expertise, product managers, and CXOs.\

Instructions for Analysis and Response:\
To ensure your response is comprehensive and insightful, please follow these structured steps:\

Understand and Summarize: Scrutinize the primary message to grasp its essence. Break it down and offer a succinct summary.\

Relevance Check: Evaluate if the message pertains to Atlas' key business domains or intersects with the expertise of founders, PMs, developers, and marketers in our industry.\

Focused Response: Based on your analysis, address the below points. Keep your responses direct, avoiding repeating the questions or adding extraneous commentary.\

Response Formatting: Utilize the JSON format for your reply, organizing your feedback as per the following schema:\

ThreadSummary: A concise overview of the entire discussion thread.\

QuestionResolved: Indicate with 'yes', 'no', or 'No question asked' whether the initial query has been addressed.\

SuggestionsSummary: Summarize the advice or solutions proposed by others in the thread.\

TopicForDiscussion: Propose a new discussion point or question that could further engage the community or illuminate additional insights.\

ArticleOpportunity: State 'yes' or 'no' to signal if this thread presents a chance for a deeper article exploration on the subject.\

ArticleHeading: If there's an article opportunity, suggest a compelling title.\

SuitableParticipant: Identify the most fitting role from Atlas (e.g., 'Product Manager', 'Founders', 'CXOs', 'No one', or 'Someone else - please specify') to join this conversation.\

SuggestedReply: As a member of Atlas, generate a response that the suitable member can respond to. These are the instructions for the response:\
a) Develop a thorough understanding of talking points that can be used by the suitable member
b) Make the response more humanized
c) Avoid mentioning Atlas or the suitable member unless necessary.
d) Add more details if the message is important and very relevant to Atlas.
e) Don't do any icebreaker, show interests or ask questions unless very very necessary.

Response Format\
Conclude by presenting your analysis in a JSON format, incorporating the following keys:\

ThreadSummary, QuestionResolved, SuggestionsSummary, TopicForDiscussion, ArticleOpportunity, ArticleHeading, SuitableParticipant, SuggestedReply

An example of response:\
{{
"ThreadSummary": "The initial message is a request from a user to change their email address associated with their Support-Driven Slack account. The thread includes a response from a community moderator explaining that the user can't change the email on the free plan, but they can deactivate the current account and send an invite to the new email. The user later confirms they were able to swap over the email address.",
"QuestionResolved": "Yes",
"SuggestionsSummary": "The moderator provided a solution to the user's request by explaining the process of deactivating the current account and sending an invite to the new email address. They also noted that the user should be able to change the email address themselves on the account settings page.",
"TopicForDiscussion": "This thread doesn't present a new discussion point, as it was a straightforward request and resolution. However, it could be used to create content around managing Slack accounts, such as a guide on changing email addresses or dealing with account transitions.",
"ArticleOpportunity": "No",
"ArticleHeading": "N/A",
"SuitableParticipant": "No one from Atlas needs to participate in this conversation, as it was a simple request that was successfully resolved.",
"SuggestedReply": "N/A"
}}

Do not use coding syntax markers like <```json> around your response.\
Do not add extra commentary except the JSON response.\
Take utmost care in using double quotes in the response.\

Message:
<{thread_text}>"""
    response = get_completion(prompt)
    return response

def process_for_generating_summary(input_file_path, output_file_path):
    with open(input_file_path, newline='', encoding='utf-8') as csvfile, \
         open(output_file_path, mode='w', newline='', encoding='utf-8') as output_file:

        reader = csv.DictReader(csvfile)
        output_fieldnames = [ "Thread Id", "Date & Time", "Thread Date & Time", "Primary Message", "ThreadSummary", "QuestionResolved", "SuggestionsSummary",
                             "TopicForDiscussion", "ArticleOpportunity", "ArticleHeading", "SuitableParticipant", "SuggestedReply"]
        writer = csv.DictWriter(output_file, fieldnames=output_fieldnames)
        writer.writeheader()

        current_thread = []

        for row in reader:        
            if row['Message Type'] == 'Primary Message':
                if current_thread:
                    response = generate_summary(current_thread)
                    print(response)
                    try:
                        summary_response_json = json.loads(json.loads(response)["response"])
                    except json.JSONDecodeError:
                        print(f"Error parsing JSON: {response}")
                        continue
                    primary_message_row = current_thread[0]

                    output_row = {
                        'Date & Time': primary_message_row['Date & Time'],
                        'Thread Date & Time': primary_message_row['Thread Date & Time'],
                        'Thread Id': primary_message_row['Thread Id'],
                        'Primary Message': primary_message_row['Text Message']
                    }
                    output_row.update(summary_response_json)
                    writer.writerow(output_row)

                current_thread = [row]
            else:
                current_thread.append(row)

        if current_thread:
            response = generate_summary(current_thread)
            print(response)
            try:
                summary_response_json = json.loads(json.loads(response)["response"])
            except json.JSONDecodeError:
                print(f"Error parsing JSON: {response}")
            primary_message_row = current_thread[0]

            output_row = {
                'Date & Time': primary_message_row['Date & Time'],
                'Thread Date & Time': primary_message_row['Thread Date & Time'],
                'Thread Id': primary_message_row['Thread Id'],
                'Primary Message': primary_message_row['Text Message']
            }
            output_row.update(summary_response_json)
            writer.writerow(output_row)

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        tmpkey = key.replace('/', '')
        csv_file_download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
        newkey = tmpkey.replace('.csv', '')
        pri_upload_path = '/tmp/{}_primary_messages_analyzed.csv'.format(newkey)
        sum_upload_path = '/tmp/{}_thread_summarized.csv'.format(newkey)
        s3_client.download_file(bucket, key, csv_file_download_path)
        process_csv(csv_file_download_path, pri_upload_path)
        process_for_generating_summary(csv_file_download_path, sum_upload_path)
        upload_key = key.replace('.csv', '')
        s3_client.upload_file(pri_upload_path, 'slackdumpanalyzedfilescsv', '{}_primary_messages_analyzed.csv'.format(upload_key))
        s3_client.upload_file(sum_upload_path, 'slackdumpthreadsummarizedfilescsv', '{}_thread_summarized.csv'.format(upload_key))

        sheet_name = get_sheet_name_from_key(key)
        spreadsheet_id = get_spreadsheet_id(sheet_name)
        if spreadsheet_id:
            append_data_to_sheet(spreadsheet_id, csv_file_download_path, sheet_name)