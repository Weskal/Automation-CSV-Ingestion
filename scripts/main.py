from pathlib import Path
import os
import json

aws_creds_path = Path(__file__).parent.parent / "config" / "s3_config.json"
incoming_data_input = Path(__file__).parent.parent / "data" / "incoming"
processed_data_outuput = Path(__file__).parent.parent / "data" / "processed"

with open(aws_creds_path) as f:
    aws_config = json.load(f)
