from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_hello():
    print("سلام! این یه DAG ساده است.")

def print_date():
    from datetime import datetime
    now = datetime.now()
    print(f"تاریخ و زمان فعلی: {now}")

def do_math():
    a = 5
    b = 3
    print(f"نتیجه جمع {a} + {b} = {a + b}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 22, 0, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='simple_example_dag',
    default_args=default_args,
    schedule='* * * * *',
    catchup=False,
    tags=['example'],
) as dag:

    task_hello = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )

    task_date = PythonOperator(
        task_id='print_date',
        python_callable=print_date
    )

    task_math = PythonOperator(
        task_id='do_math',
        python_callable=do_math
    )

# تعریف ترتیب اجرای کارها
task_hello >> task_date >> task_math
