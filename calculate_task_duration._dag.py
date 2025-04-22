from packaging.version import Version

from airflow import __version__
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.settings import Session

import pendulum


def get_base_airflow_version_tuple() -> tuple[int, int, int]:
    airflow_version = Version(__version__)
    return airflow_version.major, airflow_version.minor, airflow_version.micro


def is_airflow_2_10_plus():
    version = get_base_airflow_version_tuple()
    if version >= (2, 10, 0):
        return ">=2.10"

    return "<2.10"


query_dict = {
    ">=2.10": """with q1 as (
                SELECT 
                    SUM(EXTRACT(EPOCH FROM (end_date - start_date)) / 60) AS total_duration_minutes
                FROM 
                    task_instance
                WHERE 
                    start_date >= DATE('{start_date}')
                    AND end_date < DATE('{end_date}')
                    AND end_date IS NOT NULL
                    AND start_date IS NOT NULL

                ),
                q2 as (
                    SELECT 
                    SUM(EXTRACT(EPOCH FROM (end_date - start_date)) / 60) AS total_duration_minutes
                FROM 
                    task_instance_history
                WHERE 
                    start_date >= DATE('{start_date}')
                    AND end_date < DATE('{end_date}')
                    AND end_date IS NOT NULL
                    AND start_date IS NOT NULL

                )
                select sum(total_duration_minutes) as total_duration_minutes
                from (
                    select total_duration_minutes from q1
                    union all
                    select total_duration_minutes from q2
                ) as combined_results;""",
    "<2.10": """SELECT 
                SUM(EXTRACT(EPOCH FROM (end_date - start_date)) / 60) AS total_duration_minutes
            FROM 
                task_instance
            WHERE 
                start_date >= DATE('{start_date}')
                AND end_date < DATE('{end_date}')
                AND end_date IS NOT NULL
                AND start_date IS NOT NULL;
            """,
}


@dag(
    start_date=pendulum.datetime(2024, 3, 1),
    schedule_interval=None,
    catchup=False,
    default_args={"owner": "airflow", "retries": 2},
    tags=["metadata", "task_duration"],
    params={
        "start_date": Param(
            default=pendulum.now().subtract(months=2).to_date_string(),
            description="Minimum start_date for task instances",
            examples=["2024-01-01"],
            format="date",
        ),
        "end_date": Param(
            default=pendulum.now().to_date_string(),
            description="Max end date for task instances",
            examples=["2024-01-01"],
            format="date",
        ),
    },
    description="DAG to calculate total task duration in minutes for tasks that started and ended within March 2024 using Airflow session.",
)
def calculate_task_duration():
    """
    DAG to calculate the total duration in minutes for tasks that started and ended within March 2024
    using the Airflow metadata database via the session variable.
    """

    @task
    def calculate_duration(**kwargs):
        """
        Query the Airflow metadata database using the session variable to calculate the total task duration in minutes
        for tasks that started and ended within March 2024.
        """
        session = Session()
        query_key = is_airflow_2_10_plus()
        query = query_dict[query_key]
        params = kwargs["params"]
        query = query.format(**params)
        print(query)
        result = session.execute(query).fetchone()
        total_duration_minutes = result[0]
        print(f"Total task duration in minutes for interval: {total_duration_minutes}")
        return {
            **params,
            "total_duration_minutes": total_duration_minutes,
        }

    calculate_duration()


calculate_task_duration()
