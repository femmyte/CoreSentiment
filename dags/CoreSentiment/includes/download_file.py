import requests
base_path = "/opt/airflow/dags/CoreSentiment/files"

def download_file():  
        url = "https://dumps.wikimedia.org/other/pageviews/2024/2024-10/pageviews-20241012-130000.gz"
        file_name = url.split('/')[-1]
        local_filename = f'{base_path}/{file_name}'
        req = requests.get(url, stream=True)
        with open(local_filename, 'wb') as f:
            for chunk in req.raw.stream(1024, decode_content=False):
                if chunk:
                    f.write(chunk)