import pandas as pd

user_data = pd.read_csv('hdfs://localhost:9000/spark/data.csv')
course_info = pd.read_csv('hdfs://localhost:9000/spark/Course_info.csv')

user_data = user_data[['id', 'course_id', 'rate', 'display_name']]\
            .rename(columns={'id': 'user_id', 'display_name': 'user_name'}).drop_duplicates()
user_data = user_data.dropna(subset=['user_id', 'course_id', 'rate'])

course_info = course_info[['id', 'title']].rename(columns={'id': 'course_id'}).drop_duplicates()
course_info = course_info.dropna(subset=['course_id'])

merged_data = pd.merge(user_data, course_info, on='course_id', how='left')

merged_data['title'] = merged_data['title'].fillna('Unknown Course')
merged_data['user_name'] = merged_data['user_name'].fillna('Unknown name')

merged_data.to_csv('hdfs://localhost:9000/sparkfinal_data2.csv', index=False)

print("Data cleaning and saving to CSV complete.")
