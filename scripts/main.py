from pathlib import Path
import os
from dotenv import load_dotenv
import json
from s3_uploader import upload_demand_to_s3
from email_downloader import fetch_csv_attachments
import logging

os.makedirs('../data/incoming', exist_ok=True)
os.makedirs('../data/processed', exist_ok=True)

aws_creds_path = Path(__file__).parent.parent / "config" / "s3_config.json"
incoming_data_input = Path(__file__).parent.parent / "data" / "incoming"
processed_data_outuput = Path(__file__).parent.parent / "data" / "processed"

load_dotenv()

email_config = {
    "email_user": os.getenv("EMAIL_USER"),
    "email_pass": os.getenv("EMAIL_PASS"),
    "imap_server": os.getenv("IMAP_SERVER", "imap.gmail.com"),
    "mailbox": "INBOX",
    "output_dir": "data/incoming"
}

with open(aws_creds_path) as f:
    aws_config = json.load(f)

fetch_csv_attachments(email_config)
print("=="*30)
upload_results = upload_demand_to_s3(aws_config, incoming_data_input, processed_data_outuput)
print(upload_results)

