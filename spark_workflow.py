#!/usr/bin/env python3

from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner
import paramiko
from typing import Dict, Optional
import logging

CLUSTER_CONFIG = {
    "host": "192.168.1.15",
    "username": "team",
    "spark_user": "spark",
    "spark_home": "/opt/spark",
    "java_home": "/usr/lib/jvm/java-11-openjdk-amd64",
    "script_path": "/opt/spark/scripts/spark-data-processing.py",
    "hdfs_namenode": "hdfs://192.168.1.15:9000",
}

logger = logging.getLogger(__name__)


@task(name="Check Cluster Connection", retries=2, retry_delay_seconds=5)
def check_cluster_connection(host: str, username: str) -> Dict[str, str]:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        client.connect(hostname=host, username=username, timeout=10)

        stdin, stdout, stderr = client.exec_command(
            "curl -s http://localhost:8088/ws/v1/cluster/info"
        )
        yarn_status = "RUNNING" if stdout.channel.recv_exit_status() == 0 else "UNAVAILABLE"

        stdin, stdout, stderr = client.exec_command(f"hdfs dfs -test -d /")
        hdfs_status = "RUNNING" if stdout.channel.recv_exit_status() == 0 else "UNAVAILABLE"

        stdin, stdout, stderr = client.exec_command("netstat -tln | grep 9083")
        hive_status = "RUNNING" if stdout.channel.recv_exit_status() == 0 else "UNAVAILABLE"

        return {
            "yarn": yarn_status,
            "hdfs": hdfs_status,
            "hive_metastore": hive_status,
        }

    finally:
        client.close()


@task(name="Create HDFS Directory", retries=1)
def prepare_hdfs_environment(host: str, username: str, hdfs_path: str) -> bool:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        client.connect(hostname=host, username=username, timeout=10)
        command = f"hdfs dfs -mkdir -p {hdfs_path}"
        stdin, stdout, stderr = client.exec_command(command)
        exit_status = stdout.channel.recv_exit_status()

        if exit_status not in [0, 1]:
            error = stderr.read().decode()
            raise Exception(f"HDFS directory creation failed: {error}")

        return True

    finally:
        client.close()


@task(name="Submit Spark Job to YARN", retries=1)
def submit_spark_job(
    host: str,
    username: str,
    spark_user: str,
    spark_home: str,
    java_home: str,
    script_path: str
) -> Dict[str, str]:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        client.connect(hostname=host, username=username, timeout=10)

        spark_submit_cmd = f"""
        sudo -u {spark_user} JAVA_HOME={java_home} {spark_home}/bin/spark-submit \
            --master yarn \
            --deploy-mode client \
            --driver-memory 1g \
            --executor-memory 1g \
            --executor-cores 1 \
            --num-executors 2 \
            --conf spark.sql.catalogImplementation=hive \
            --conf spark.sql.warehouse.dir=hdfs://{host}:9000/user/hive/warehouse \
            {script_path}
        """

        stdin, stdout, stderr = client.exec_command(
            spark_submit_cmd, timeout=300)

        output_lines = []
        for line in stdout:
            line = line.strip()
            output_lines.append(line)

        exit_status = stdout.channel.recv_exit_status()

        if exit_status != 0:
            error_lines = stderr.readlines()
            error_output = "\n".join([e.strip() for e in error_lines[-20:]])
            raise Exception(
                f"Spark job failed with exit code {exit_status}: {error_output}")

        application_id = None
        for line in output_lines:
            if "application_" in line.lower():
                parts = line.split()
                for part in parts:
                    if "application_" in part:
                        application_id = part
                        break

        return {
            "status": "SUCCESS",
            "application_id": application_id or "N/A",
            "exit_code": exit_status,
        }

    finally:
        client.close()


@task(name="Verify Hive Tables", retries=1)
def verify_hive_tables(
    host: str,
    username: str,
    spark_user: str,
    spark_home: str,
    java_home: str,
    tables: list
) -> Dict[str, bool]:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        client.connect(hostname=host, username=username, timeout=10)
        results = {}

        for table in tables:
            check_cmd = f"""
            sudo -u {spark_user} JAVA_HOME={java_home} {spark_home}/bin/spark-sql \
                --master yarn \
                --deploy-mode client \
                -e "SELECT COUNT(*) as row_count FROM {table} LIMIT 1;"
            """

            stdin, stdout, stderr = client.exec_command(check_cmd, timeout=60)
            exit_status = stdout.channel.recv_exit_status()
            results[table] = exit_status == 0

        return results

    finally:
        client.close()


@task(name="Generate Summary Report")
def generate_summary_report(
    cluster_status: Dict[str, str],
    spark_job_result: Dict[str, str],
    tables_status: Dict[str, bool]
) -> str:
    report = []
    report.append("SPARK WORKFLOW EXECUTION REPORT")
    report.append("=" * 40)

    report.append("CLUSTER STATUS:")
    for service, status in cluster_status.items():
        report.append(f"  {service}: {status}")

    report.append("SPARK JOB:")
    report.append(f"  Status: {spark_job_result.get('status', 'UNKNOWN')}")
    if spark_job_result.get('application_id'):
        report.append(
            f"  Application ID: {spark_job_result['application_id']}")

    report.append("CREATED TABLES:")
    for table, exists in tables_status.items():
        status = "EXISTS" if exists else "MISSING"
        report.append(f"  {table}: {status}")

    report.append("=" * 40)

    return "\n".join(report)


@flow(
    name="Spark Data Processing Workflow",
    task_runner=ThreadPoolTaskRunner(max_workers=1),
)
def spark_data_processing_flow(
    host: Optional[str] = None,
    username: Optional[str] = None,
) -> Dict:
    cluster_host = host or CLUSTER_CONFIG["host"]
    cluster_username = username or CLUSTER_CONFIG["username"]

    cluster_status = check_cluster_connection(cluster_host, cluster_username)

    hdfs_path = "/data/sample"
    prepare_hdfs_environment(cluster_host, cluster_username, hdfs_path)

    spark_job_result = submit_spark_job(
        host=cluster_host,
        username=cluster_username,
        spark_user=CLUSTER_CONFIG["spark_user"],
        spark_home=CLUSTER_CONFIG["spark_home"],
        java_home=CLUSTER_CONFIG["java_home"],
        script_path=CLUSTER_CONFIG["script_path"],
    )

    tables_to_verify = [
        "spark_demo.sales_data_processed",
        "spark_demo.sales_by_category",
        "spark_demo.sales_by_month",
    ]

    tables_status = verify_hive_tables(
        host=cluster_host,
        username=cluster_username,
        spark_user=CLUSTER_CONFIG["spark_user"],
        spark_home=CLUSTER_CONFIG["spark_home"],
        java_home=CLUSTER_CONFIG["java_home"],
        tables=tables_to_verify,
    )

    summary = generate_summary_report(
        cluster_status=cluster_status,
        spark_job_result=spark_job_result,
        tables_status=tables_status,
    )

    return {
        "cluster_status": cluster_status,
        "spark_job_result": spark_job_result,
        "tables_status": tables_status,
        "summary": summary,
    }


if __name__ == "__main__":
    result = spark_data_processing_flow()
