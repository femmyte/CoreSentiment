def save_highest_pageviews_to_file(company, pageviews):
    with open(f'/opt/airflow/dags/CoreSentiment/files/result_file.txt', 'w') as f:
        f.write(f"Company with the highest pageviews: {company} with {pageviews} pageviews.\n")