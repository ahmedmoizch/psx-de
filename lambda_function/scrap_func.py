import io
import os
import boto3
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d-%H%M%S")
    
    bucket_name = os.environ.get('S3_BUCKET_NAME', 'XXX')
    key_prefix = 'lambda_psx_csv/'
    url = "https://dps.psx.com.pk/market-watch"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        # removing unwanted elements like xd, xb..
        # using html tags
        classes_to_remove = ['tag tag--skim tag--def', 'tag tag--skim tag--xd', 'tag tag--skim tag--xb']
        for element in soup.find_all(class_=classes_to_remove):
            element.decompose()

        # Parse tables safely
        html_data = io.StringIO(str(soup))
        tables = pd.read_html(html_data)

        if not tables:
            raise ValueError("No tables were found on the target web page.")

        dfs = tables[0]

        # table to CSV
        csv_buffer = io.StringIO()
        dfs.to_csv(csv_buffer, index=False)
        
        # full_s3_key = f"{key_prefix}psx_current_{timestamp}.csv"
        full_s3_key = key_prefix + f'psx_current_{timestamp}.csv'

        s3_client.put_object(
            Bucket=bucket_name,
            Key=full_s3_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )

        return {
            "statusCode": 200,
            "body": f"Successfully stored data at s3://{bucket_name}/{full_s3_key}"
        }

    except Exception as e:
        print(f"Error scraping PSX data: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Failed execution: {str(e)}"
        }
