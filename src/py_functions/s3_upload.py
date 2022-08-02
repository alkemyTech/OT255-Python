from airflow.hooks.S3_hook import S3Hook

def upload_univ_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, 
    key=key, 
    bucket_name=bucket_name, 
    replace=True)