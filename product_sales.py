from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from minio import Minio
import pandas as pd, glob, os, airflow.utils.dates, sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from botocore.exceptions import ClientError

# Create Minio client
minio_client = Minio(
    "docker-minio:9000",
    access_key="F27De9ca8pOR4LG1",
    secret_key="mEwGTU3F2VHMClMYzU35ju52WrCdf8f0",
    secure=False
)

bucket_name = "bucket-bucket"

conn = PostgresHook(postgres_conn_id='postgre_airflow').get_conn()
cursor = conn.cursor()

# The mail addresses and password
sender_address = 'ahmadfadilfatanm@gmail.com'
receiver_address = 'ahmadfadilfatanm@gmail.com'
appPassword = 'dqxacxokawmpxufw'

def format_currency(number):
    currency = str(number)

    if len(currency) <= 3:
        return 'Rp '+ currency
    else:
        start = currency[-3:]
        end = currency[:-3]

    return format_currency(end) + '.' + start

def upload_to_minio():
    objects = minio_client.list_objects(bucket_name, recursive=True)

    countFile = 0
    arrFile = []
    for obj in objects:
        arrFile.append(obj.object_name)

    countFile = len(arrFile)
    
    if countFile < 1:
        # Upload file to Minio
        minio_client.fput_object("bucket-bucket", "scanner_data.csv", "/opt/airflow/dags/scanner_data.csv")
        print("Berhasil mengupload file. . .")
    else:
        print("Sudah ada file pada server minio. . .")

def download_from_minio():
    objects = minio_client.list_objects(bucket_name, recursive=True)

    countFile = 0
    arrFile = []
    for obj in objects:
        arrFile.append(obj.object_name)

    countFile = len(arrFile)
    
    if countFile < 1:
        print("Tidak ada file yang didownload. . .")
    else:
        # Download file from Minio
        minio_client.fget_object("bucket-bucket", "scanner_data.csv", "/opt/airflow/dags/scanner_data.csv")
        print("Berhasil mendownload file. . .")
    
def read_from_minio():
    objects = minio_client.list_objects(bucket_name, recursive=True)
    
    for obj in objects:
        # Read file from Minio
        response = minio_client.get_object(obj.bucket_name, obj.object_name)

        df = pd.read_csv(response)

        for index, row in df.iterrows():
            cursor.execute('SELECT * FROM project_sales where sku = %s', (row["sku"],))
            countRows   = cursor.rowcount

            if countRows < 1:
                insert_data(row["sku"], row["sold"], row["price"], row["baseprice"])
            else:
                print("SKU "+row['sku']+" Data Sudah Ada")        

def insert_data(sku, sold, price, baseprice):
    params = (sku, sold, price, baseprice)

    cursor.execute("INSERT INTO project_sales (sku, sold, price, baseprice) VALUES (%s, %s, %s, %s)", params)
    conn.commit()
    print("Data Telah Di Insert")

def parsing_data():
    print("Data Telah di Parsing.")

    cursor.execute('SELECT * FROM project_sales')
    result = cursor.fetchall()

    sold = []
    price = []
    baseprice = []
    
    for res in result:
        sold.append(res[1])
        price.append(res[2])
        baseprice.append(res[3])

    return sold, price, baseprice


def send_email():
    response = parsing_data()
    
    jumlah_sold = sum(response[0])
    jumlah_price = sum(response[1])
    jumlah_baseprice = sum(response[2])

    profit = jumlah_sold * (jumlah_price - jumlah_baseprice)

    # Create the body of the message (a plain-text and an HTML version).
    html = """\
    <html>
        <head></head>
        <body>
            <p>Summary : <br>
            </p>
            <table border='1'>
                <thead>
                    <tr>
                        <th>Jumlah Sold</th>
                        <th>Jumlah Price</th>
                        <th>Jumlah Baseprice</th>
                        <th>Keuntungan</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>"""+format_currency(str(jumlah_sold))+"""</td>
                        <td>"""+format_currency(str(jumlah_price))+"""</td>
                        <td>"""+format_currency(str(jumlah_baseprice))+"""</td>
                        <td>"""+format_currency(str(profit))+"""</td>
                    </tr>
                </tbody>
            </table>
        </body>
    </html>
    """

    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = receiver_address
    message['Subject'] = 'Summary Profit' #
    html_content = MIMEText(html, 'html')

    message.attach(html_content)

    session = smtplib.SMTP('smtp.gmail.com', 587) 
    session.starttls() 
    session.login(sender_address, appPassword)
    text = message.as_string()
    session.sendmail(sender_address, receiver_address, text)
    session.quit()
    
    print('Email telah terkirim.')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 9),
}

with DAG('project_sales', default_args=default_args, schedule_interval='@daily') as dag:
    
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
            create table if not exists project_sales (
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
