from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from minio import Minio
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from botocore.exceptions import ClientError

# Minio configuration
MINIO_HOST = 'minio_host'
MINIO_ACCESS_KEY = 'minio_access_key'
MINIO_SECRET_KEY = 'minio_secret_key'
MINIO_BUCKET = 'minio_bucket'
MINIO_PREFIX = 'minio_prefix/'

# PostgreSQL configuration
POSTGRES_CONN_ID = 'postgres_conn_id'
POSTGRES_TABLE = 'postgres_table'

def read_csv_from_minio():
    # Connect to Minio
    client = Minio(
        MINIO_HOST,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # Download the latest CSV file
    objects = client.list_objects(MINIO_BUCKET, prefix=MINIO_PREFIX)
    latest_object = max(objects, key=lambda x: x.last_modified)
    file_path = '/tmp/' + latest_object.object_name.split('/')[-1]
    client.fget_object(MINIO_BUCKET, latest_object.object_name, file_path)

    # Read the CSV file
    df = pd.read_csv(file_path)

    return df

def insert_data_to_postgres():
    # Read the CSV file from Minio
    df = read_csv_from_minio()

    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Create the table if not exists
    pg_hook.run(f"""
        CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
            sku VARCHAR NOT NULL PRIMARY KEY,
            sold INTEGER NOT NULL,
            price INTEGER NOT NULL,
            baseprice INTEGER NOT NULL
        )
    """)

    # Insert the data into the table
    for index, row in df.iterrows():
        sku = row['sku']
        sold = row['sold']
        price = row['price']
        baseprice = row['baseprice']
        pg_hook.run(f"""
            INSERT INTO {POSTGRES_TABLE} (sku, sold, price, baseprice)
            VALUES ('{sku}', {sold}, {price}, {baseprice})
            ON CONFLICT (sku) DO UPDATE SET sold = excluded.sold, price = excluded.price, baseprice = excluded.baseprice
        """)

    print('Data berhasil disimpan di PostgreSQL')

def send_email():
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Read the data from the table
    rows = pg_hook.get_records(f"""
        SELECT SUM(sold), SUM(price), SUM(baseprice)
        FROM {POSTGRES_TABLE}
    """)

    # Calculate the profit
    total_sold = rows[0][0]
    total_price = rows[0][1]
    total_baseprice = rows[0][2]
    profit = total_sold * (total_price - total_baseprice)

    # Create the HTML content for the email
    html = f"""
        <html>
            <head></head>
            <body>
                <p>Berikut adalah summarynya:</p>
                <table border='1'>
                    <thead>
                        <tr>
                            <th>Total Sold</th>
                            <th>Total Price</th>
                            <th>Total Baseprice
     <th>Profit</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>"""+format_currency(str(total_sold))+"""</td>
                        <td>"""+format_currency(str(total_price))+"""</td>
                        <td>"""+format_currency(str(total_baseprice))+"""</td>
                        <td>"""+format_currency(str(profit))+"""</td>
                    </tr>
                </tbody>
            </table>
        </body>
    </html>
    """

    #Setup the MIME
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = receiver_address
    message['Subject'] = 'Summary Profit' # The subject line

    # The body and the attachments for the mail
    # Record the MIME types of both parts - text/plain and text/html.
    html_content = MIMEText(html, 'html')

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    message.attach(html_content)

    #Create SMTP session for sending the mail
    session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
    session.starttls() #enable security
    session.login(sender_address, appPassword) #login with mail_id and password
    text = message.as_string()
    session.sendmail(sender_address, receiver_address, text)
    session.quit()
    
    print('Email telah terkirim.')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 7),
}

with DAG('project_2', default_args=default_args, schedule_interval='@daily') as dag:
    
    # Task to upload file to Minio
    upload_file = PythonOperator(
        task_id='upload_file',
        python_callable=upload_to_minio
    )

    # Task to download file from Minio
    download_file = PythonOperator(
        task_id='download_file',
        python_callable=download_from_minio
    )

    createTable = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'postgre_airflow',
        sql = '''
            create table if not exists project_2 (
                sku VARCHAR NOT NULL PRIMARY KEY,
                sold INTEGER NOT NULL not null default 0,
                price INTEGER NOT NULL not null default 0,
                baseprice INTEGER NOT NULL not null default 0
            );
        ''',
        dag = dag
    )

    process_read_file = PythonOperator(
        task_id='read_file',
        python_callable=read_from_minio
    )

    process_send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_email
    )

    # Set task dependencies
    upload_file >> download_file >> createTable >> process_read_file >> process_send_email
