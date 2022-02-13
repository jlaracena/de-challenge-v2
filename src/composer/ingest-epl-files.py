import datetime
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator 
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator


today                                = datetime.datetime.now()
project_id                           = "peppy-oven-288419"
dataset_id                           = "epl_dataset"
bucket_landing                       = "gs://epl-landing-file-5886940c92ed"
bucket_sending                       = "gs://epl-sending-file-b9e6d09605ea"

default_args = {
    'owner': 'EPL-Analitycs',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(today.year, today.month, today.day),
}

with DAG(
    'ingest-epl-files',
    default_args=default_args,
    description='DAG for EPL Files',
    schedule_interval=None, 
    tags=['ingestion'],
) as dag:

    start           = DummyOperator(task_id='start')
    end             = DummyOperator(task_id='end')

    #Clean format and invalid columns names
    load_data_temp09 = BashOperator (
        task_id = "load_data_temp09",
        bash_command=f'gsutil cp {bucket_landing}/NDseason-0910_json.json . \
        && gsutil cp {bucket_landing}/schema-epl.json . \
        && sed -i "s/BbAv<2.5/BbAvminus25/g;s/BbAv>2.5/BbAvmore25/g;s/BbMx<2.5/BbMxminus25/g;s/BbMx>2.5/BbMxmore35/g" NDseason-0910_json.json \
        && gsutil cp NDseason-0910_json.json {bucket_landing}/CNDseason-0910_json.json \
        && bq load --source_format=NEWLINE_DELIMITED_JSON --schema=./schema-epl.json {dataset_id}.raw_season_0910 {bucket_landing}/CNDseason-0910_json.json  \
        && rm NDseason-0910_json.json schema-epl.json',
        dag=dag
    )

    load_data_temp10 = BashOperator (
        task_id = "load_data_temp10",
        bash_command=f'gsutil cp {bucket_landing}/NDseason-1011_json.json . \
        && gsutil cp {bucket_landing}/schema-epl.json . \
        && sed -i "s/BbAv<2.5/BbAvminus25/g;s/BbAv>2.5/BbAvmore25/g;s/BbMx<2.5/BbMxminus25/g;s/BbMx>2.5/BbMxmore35/g" NDseason-1011_json.json \
        && gsutil cp NDseason-1011_json.json {bucket_landing}/CNDseason-1011_json.json \
        && bq load --source_format=NEWLINE_DELIMITED_JSON --schema=./schema-epl.json {dataset_id}.raw_season_1011 {bucket_landing}/CNDseason-1011_json.json  \
        && rm NDseason-1011_json.json schema-epl.json',
        dag=dag
    )

    load_data_temp11 = BashOperator (
        task_id = "load_data_temp11",
        bash_command=f'gsutil cp {bucket_landing}/NDseason-1112_json.json . \
        && gsutil cp {bucket_landing}/schema-epl.json . \
        && sed -i "s/BbAv<2.5/BbAvminus25/g;s/BbAv>2.5/BbAvmore25/g;s/BbMx<2.5/BbMxminus25/g;s/BbMx>2.5/BbMxmore35/g" NDseason-1112_json.json \
        && gsutil cp NDseason-1112_json.json {bucket_landing}/CNDseason-1112_json.json \
        && bq load --source_format=NEWLINE_DELIMITED_JSON --schema=./schema-epl.json {dataset_id}.raw_season_1112 {bucket_landing}/CNDseason-1112_json.json  \
        && rm NDseason-1112_json.json schema-epl.json',
        dag=dag
    )

    load_data_temp12 = BashOperator (
        task_id = "load_data_temp12",
        bash_command=f'gsutil cp {bucket_landing}/NDseason-1213_json.json . \
        && gsutil cp {bucket_landing}/schema-epl.json . \
        && sed -i "s/BbAv<2.5/BbAvminus25/g;s/BbAv>2.5/BbAvmore25/g;s/BbMx<2.5/BbMxminus25/g;s/BbMx>2.5/BbMxmore35/g" NDseason-1213_json.json \
        && gsutil cp NDseason-1213_json.json {bucket_landing}/CNDseason-1213_json.json \
        && bq load --source_format=NEWLINE_DELIMITED_JSON --schema=./schema-epl.json {dataset_id}.raw_season_1213 {bucket_landing}/CNDseason-1213_json.json  \
        && rm NDseason-1213_json.json schema-epl.json',
        dag=dag
    )

    load_data_temp13 = BashOperator (
        task_id = "load_data_temp13",
        bash_command=f'gsutil cp {bucket_landing}/NDseason-1314_json.json . \
        && gsutil cp {bucket_landing}/schema-epl.json . \
        && sed -i "s/BbAv<2.5/BbAvminus25/g;s/BbAv>2.5/BbAvmore25/g;s/BbMx<2.5/BbMxminus25/g;s/BbMx>2.5/BbMxmore35/g" NDseason-1314_json.json \
        && gsutil cp NDseason-1314_json.json {bucket_landing}/CNDseason-1314_json.json \
        && bq load --source_format=NEWLINE_DELIMITED_JSON --schema=./schema-epl.json {dataset_id}.raw_season_1314 {bucket_landing}/CNDseason-1314_json.json  \
        && rm NDseason-1314_json.json schema-epl.json',
        dag=dag
    )

    load_data_temp14 = BashOperator (
        task_id = "load_data_temp14",
        bash_command=f'gsutil cp {bucket_landing}/NDseason-1415_json.json . \
        && gsutil cp {bucket_landing}/schema-epl.json . \
        && sed -i "s/BbAv<2.5/BbAvminus25/g;s/BbAv>2.5/BbAvmore25/g;s/BbMx<2.5/BbMxminus25/g;s/BbMx>2.5/BbMxmore35/g" NDseason-1415_json.json \
        && gsutil cp NDseason-1415_json.json {bucket_landing}/CNDseason-1415_json.json \
        && bq load --source_format=NEWLINE_DELIMITED_JSON --schema=./schema-epl.json {dataset_id}.raw_season_1415 {bucket_landing}/CNDseason-1415_json.json  \
        && rm NDseason-1415_json.json schema-epl.json',
        dag=dag
    )

    load_data_temp15 = BashOperator (
        task_id = "load_data_temp15",
        bash_command=f'gsutil cp {bucket_landing}/NDseason-1516_json.json . \
        && gsutil cp {bucket_landing}/schema-epl.json . \
        && sed -i "s/BbAv<2.5/BbAvminus25/g;s/BbAv>2.5/BbAvmore25/g;s/BbMx<2.5/BbMxminus25/g;s/BbMx>2.5/BbMxmore35/g" NDseason-1516_json.json \
        && gsutil cp NDseason-1516_json.json {bucket_landing}/CNDseason-1516_json.json \
        && bq load --source_format=NEWLINE_DELIMITED_JSON --schema=./schema-epl.json {dataset_id}.raw_season_1516 {bucket_landing}/CNDseason-1516_json.json  \
        && rm NDseason-1516_json.json schema-epl.json',
        dag=dag
    )

    load_data_temp16 = BashOperator (
        task_id = "load_data_temp16",
        bash_command=f'gsutil cp {bucket_landing}/NDseason-1617_json.json . \
        && gsutil cp {bucket_landing}/schema-epl.json . \
        && sed -i "s/BbAv<2.5/BbAvminus25/g;s/BbAv>2.5/BbAvmore25/g;s/BbMx<2.5/BbMxminus25/g;s/BbMx>2.5/BbMxmore35/g" NDseason-1617_json.json \
        && gsutil cp NDseason-1617_json.json {bucket_landing}/CNDseason-1617_json.json \
        && bq load --source_format=NEWLINE_DELIMITED_JSON --schema=./schema-epl.json {dataset_id}.raw_season_1617 {bucket_landing}/CNDseason-1617_json.json  \
        && rm NDseason-1617_json.json schema-epl.json',
        dag=dag
    )

    load_data_temp17 = BashOperator (
        task_id = "load_data_temp17",
        bash_command=f'gsutil cp {bucket_landing}/NDseason-1718_json.json . \
        && gsutil cp {bucket_landing}/schema-epl.json . \
        && sed -i "s/BbAv<2.5/BbAvminus25/g;s/BbAv>2.5/BbAvmore25/g;s/BbMx<2.5/BbMxminus25/g;s/BbMx>2.5/BbMxmore35/g" NDseason-1718_json.json \
        && gsutil cp NDseason-1718_json.json {bucket_landing}/CNDseason-1718_json.json \
        && bq load --source_format=NEWLINE_DELIMITED_JSON --schema=./schema-epl.json {dataset_id}.raw_season_1718 {bucket_landing}/CNDseason-1718_json.json  \
        && rm NDseason-1718_json.json schema-epl.json',
        dag=dag
    )

    load_data_temp18 = BashOperator (
        task_id = "load_data_temp18",
        bash_command=f'gsutil cp {bucket_landing}/NDseason-1819_json.json . \
        && gsutil cp {bucket_landing}/schema-epl.json . \
        && sed -i "s/BbAv<2.5/BbAvminus25/g;s/BbAv>2.5/BbAvmore25/g;s/BbMx<2.5/BbMxminus25/g;s/BbMx>2.5/BbMxmore35/g" NDseason-1819_json.json \
        && gsutil cp NDseason-1819_json.json {bucket_landing}/CNDseason-1819_json.json \
        && bq load --source_format=NEWLINE_DELIMITED_JSON --schema=./schema-epl.json {dataset_id}.raw_season_1819 {bucket_landing}/CNDseason-1819_json.json  \
        && rm NDseason-1819_json.json schema-epl.json',
        dag=dag
    )
    
    query_report1 = """
                WITH
            all_seasons AS (
            SELECT
                *,
                "0910" AS SEASON,
            FROM
                epl_dataset.raw_season_0910
            UNION ALL
            SELECT
                *,
                "1011" AS SEASON,
            FROM
                epl_dataset.raw_season_1011
            UNION ALL
            SELECT
                *,
                "1112" AS SEASON,
            FROM
                epl_dataset.raw_season_1112
            UNION ALL
            SELECT
                *,
                "1213" AS SEASON,
            FROM
                epl_dataset.raw_season_1213
            UNION ALL
            SELECT
                *,
                "1314" AS SEASON,
            FROM
                epl_dataset.raw_season_1314
            UNION ALL
            SELECT
                *,
                "1415" AS SEASON,
            FROM
                epl_dataset.raw_season_1415
            UNION ALL
            SELECT
                *,
                "1516" AS SEASON,
            FROM
                epl_dataset.raw_season_1516
            UNION ALL
            SELECT
                *,
                "1617" AS SEASON,
            FROM
                epl_dataset.raw_season_1617
            UNION ALL
            SELECT
                *,
                "1718" AS SEASON,
            FROM
                epl_dataset.raw_season_1718
            UNION ALL
            SELECT
                *,
                "1819" AS SEASON,
            FROM
                epl_dataset.raw_season_1819 )
            SELECT
            season,
            HomeTeam AS team,
            COUNT(*) AS played,
            COUNT(CASE
                WHEN FTHG > FTAG THEN 1
            END
                ) AS wins,
            COUNT(CASE
                WHEN FTAG> FTHG THEN 1
            END
                ) AS lost,
            COUNT(CASE
                WHEN FTHG = FTAG THEN 1
            END
                ) AS draws,
            SUM(FTHG) AS goalsfor,
            SUM(FTAG) AS goalsagainst,
            SUM(FTHG) - SUM(FTAG) AS goal_diff,
            SUM(
                CASE
                WHEN FTHG > FTAG THEN 3
                ELSE
                0
            END
                +
                CASE
                WHEN FTHG = FTAG THEN 1
                ELSE
                0
            END
                ) AS score
            FROM
            all_seasons
            GROUP BY
            season,
            team
            ORDER BY
            season DESC,
            score DESC,
            goal_diff DESC
            """
    
    #position table all seasons
    create_table_report_1 = BigQueryOperator(
        task_id="create_table_report_1",
        sql=query_report1,
        use_legacy_sql = False,
        write_disposition= "WRITE_TRUNCATE",
        destination_dataset_table=f"{dataset_id}.report1_position_table",
        dag=dag
    )

    save_file_in_gcs_report_1 = BigQueryToCloudStorageOperator(
        task_id="save_file_in_gcs_report_1",
        source_project_dataset_table = f"{dataset_id}.report1_position_table",
        destination_cloud_storage_uris=bucket_sending+'/report1_position_table.csv',
        export_format='CSV',
        print_header=True,
        field_delimiter=';',
        dag=dag
    )

    query_report2 = """
        WITH
        all_seasons AS (
        SELECT
            *,
            "0910" AS SEASON,
        FROM
            epl_dataset.raw_season_0910
        UNION ALL
        SELECT
            *,
            "1011" AS SEASON,
        FROM
            epl_dataset.raw_season_1011
        UNION ALL
        SELECT
            *,
            "1112" AS SEASON,
        FROM
            epl_dataset.raw_season_1112
        UNION ALL
        SELECT
            *,
            "1213" AS SEASON,
        FROM
            epl_dataset.raw_season_1213
        UNION ALL
        SELECT
            *,
            "1314" AS SEASON,
        FROM
            epl_dataset.raw_season_1314
        UNION ALL
        SELECT
            *,
            "1415" AS SEASON,
        FROM
            epl_dataset.raw_season_1415
        UNION ALL
        SELECT
            *,
            "1516" AS SEASON,
        FROM
            epl_dataset.raw_season_1516
        UNION ALL
        SELECT
            *,
            "1617" AS SEASON,
        FROM
            epl_dataset.raw_season_1617
        UNION ALL
        SELECT
            *,
            "1718" AS SEASON,
        FROM
            epl_dataset.raw_season_1718
        UNION ALL
        SELECT
            *,
            "1819" AS SEASON,
        FROM
            epl_dataset.raw_season_1819 )
        SELECT
        season,
        HomeTeam AS team,
        COUNT(*) AS played,
        SUM(FTHG) AS goalsfor,
        SUM(HS) AS shotsfor,
        ROUND(SUM(FTHG)/SUM(HS),4) AS ratio_goals_shots_for
        FROM
        all_seasons
        GROUP BY
        season,
        team
        ORDER BY
        season DESC,
        ratio_goals_shots_for DESC
        """
    
    #table of ratio goals and shots
    create_table_report_2 = BigQueryOperator(
        task_id="create_table_report_2",
        sql=query_report2,
        use_legacy_sql = False,
        write_disposition= "WRITE_TRUNCATE",
        destination_dataset_table=f"{dataset_id}.report2_ratio_goals_shots",
        dag=dag
    )

    save_file_in_gcs_report_2 = BigQueryToCloudStorageOperator(
        task_id="save_file_in_gcs_report_2",
        source_project_dataset_table = f"{dataset_id}.report2_ratio_goals_shots",
        destination_cloud_storage_uris=bucket_sending+'/report2_ratio_goals_shots.csv',
        export_format='CSV',
        print_header=True,
        field_delimiter=';',
        dag=dag
    )

    query_report3 = """
        WITH
        all_seasons AS (
        SELECT
            *,
            "0910" AS SEASON,
        FROM
            epl_dataset.raw_season_0910
        UNION ALL
        SELECT
            *,
            "1011" AS SEASON,
        FROM
            epl_dataset.raw_season_1011
        UNION ALL
        SELECT
            *,
            "1112" AS SEASON,
        FROM
            epl_dataset.raw_season_1112
        UNION ALL
        SELECT
            *,
            "1213" AS SEASON,
        FROM
            epl_dataset.raw_season_1213
        UNION ALL
        SELECT
            *,
            "1314" AS SEASON,
        FROM
            epl_dataset.raw_season_1314
        UNION ALL
        SELECT
            *,
            "1415" AS SEASON,
        FROM
            epl_dataset.raw_season_1415
        UNION ALL
        SELECT
            *,
            "1516" AS SEASON,
        FROM
            epl_dataset.raw_season_1516
        UNION ALL
        SELECT
            *,
            "1617" AS SEASON,
        FROM
            epl_dataset.raw_season_1617
        UNION ALL
        SELECT
            *,
            "1718" AS SEASON,
        FROM
            epl_dataset.raw_season_1718
        UNION ALL
        SELECT
            *,
            "1819" AS SEASON,
        FROM
            epl_dataset.raw_season_1819 )
        SELECT
        season,
        HomeTeam AS team,
        COUNT(*) AS played,
        SUM(FTAG) AS goalsagainst,
        FROM
        all_seasons
        GROUP BY
        season,
        team
        ORDER BY
        season DESC,
        goalsagainst DESC    
        """

    #table of more goals against
    create_table_report_3 = BigQueryOperator(
        task_id="create_table_report_3",
        sql=query_report3,
        use_legacy_sql = False,
        write_disposition= "WRITE_TRUNCATE",
        destination_dataset_table=f"{dataset_id}.report3_more_goals_against",
        dag=dag
    )

    save_file_in_gcs_report_3 = BigQueryToCloudStorageOperator(
        task_id="save_file_in_gcs_report_3",
        source_project_dataset_table = f"{dataset_id}.report3_more_goals_against",
        destination_cloud_storage_uris=bucket_sending+'/report3_more_goals_against.csv',
        export_format='CSV',
        print_header=True,
        field_delimiter=';',
        dag=dag
    )

    start >> load_data_temp09
    load_data_temp09  >> load_data_temp10
    load_data_temp10  >> load_data_temp11
    load_data_temp11  >> load_data_temp12
    load_data_temp12  >> load_data_temp13
    load_data_temp13  >> load_data_temp14
    load_data_temp14  >> load_data_temp15
    load_data_temp15  >> load_data_temp16
    load_data_temp16  >> load_data_temp17
    load_data_temp17  >> load_data_temp18
    load_data_temp18 >> create_table_report_1
    create_table_report_1 >> save_file_in_gcs_report_1
    save_file_in_gcs_report_1 >> create_table_report_2
    create_table_report_2 >> save_file_in_gcs_report_2
    save_file_in_gcs_report_2 >> create_table_report_3
    create_table_report_3 >> save_file_in_gcs_report_3
    save_file_in_gcs_report_3 >> end