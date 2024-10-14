base_path = "/opt/airflow/dags/CoreSentiment/files"

def fetch_page():
        with open(f'{base_path}/pageviews-20241012-130000') as f:
            search_values = ['Facebook', 'Google', 'Apple', 'Amazon', 'Microsoft']
            count = 10
            for line in f:
                for search_value in search_values:
                    if line.startswith(f'en.m {search_value} '):
                        print(f'this is line {line}')
                        my_val = line.split(' ')[-2]
                        count = count + 1
                        with open(f'/opt/airflow/dags/sql/coresentiment_schema.sql', 'a') as query_file:
                            query = f"INSERT INTO sentiment_tbl VALUES ( {count}, '{search_value}', '{my_val}');\n"
                            query_file.write(query)
