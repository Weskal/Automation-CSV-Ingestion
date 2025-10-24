import boto3
import os
import shutil
from pathlib import Path

def upload_demand_to_s3(aws_config, incoming_data_input, processed_data_outuput):

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_config["aws_access_key_id"],
        aws_secret_access_key=aws_config["aws_secret_access_key"],
        region_name=aws_config["region_name"]
    )

    key = aws_config["bucket_key_demanda"]
    bucket = aws_config["bucket"]

    files = sorted(os.listdir(incoming_data_input))
    results = {'success': [], 'failed': []}

    if not files:
        print("No files to upload")
        return results
    
    for file_name in files:
        try:
            file_path = os.path.join(incoming_data_input, file_name)
            
            # with open(file_path, 'rb') as f:
            response = s3.upload_file(
                Bucket = bucket,
                Key = f'{key}/{file_name}',
                Filename = file_path
                # Body = f
            )
            
            # if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            shutil.move(file_path, os.path.join(processed_data_outuput, file_name))
            results['success'].append(file_name)
            print(f"File {file_name} processed and uploaded successfully to bucket: {bucket}!!")
            # else:
            #     print(f"Upload fail for file: {file_name}")
            
        except Exception as e:
            results['failed'].append({'file': {file_name}, 'error': str(e)})
            print(f"Error during the file processment: {e}")
            
    return results

