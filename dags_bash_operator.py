from __future__ import annotations

import datetime
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# crontab
# 분(0-59)　　시간(0-23)　　일(1-31)　　월(1-12)　　　요일(0-7)

with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",       # 매일 0시 0분에 실행
    start_date=pendulum.datetime(2024, 6, 1, tz="Asia/Seoul"),
    catchup=False,      # true 인 경우 : 오늘날짜가 7/1 이라면 6/1~7/1 까지 누락되었던 기간이 소급 적용되어 한꺼번에 돌게됨. 
    #dagrun_timeout=datetime.timedelta(minutes=60),   # 60분 이상 돌면 TIMEOUT
    tags=["example", "example2"],
    #params={"example_key": "example_value"},
) as dag:
     # [START howto_operator_bash]
    bash_t1 = BashOperator(         # task 생성
        task_id="bash_t1",
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(         # task 생성i. 
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )
    # [END howto_operator_bash]

    bash_t1 >> bash_t2