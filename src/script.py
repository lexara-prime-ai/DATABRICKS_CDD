from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

def config_parser(config_file) -> dict:
    try:
        with open(config_file) as f:
            configuration = yaml.safe_load(f)
            output = {"job": {}}
            for k, v in configuration["job"].items():
                output["job"][k] = configuration["job"][k]
            return output
    except Exception as e:
        raise ValueError(
            "Failed to initialize Application Configuration, couldn't load YAML configuration."
        ) from e


def create_tasks(tasks_list_input: list) -> list:
    tasks_list_output = []
    for tasks_dict in tasks_list_input:
        task = jobs.Task(
            description=tasks_dict["description"],
            job_cluster_key="default_cluster",
            spark_python_task=jobs.SparkPythonTask(
                python_file=tasks_dict["python_file"],
                source=jobs.Source.GIT,
                parameters=tasks_dict["parameters"],
            ),
            task_key=tasks_dict["task_key"],
            timeout_seconds=0,
            depends_on=[
                jobs.TaskDependency(task_key=i) for i in tasks_dict.get("depends_on", [])
            ],
        )
        tasks_list_output.append(task)
    return tasks_list_output


# Parse [config] file.
config = config_parser("src/config.yaml")
# Retrieve [job] and [task] dependencies.
job = config["job"]
tasks = job["tasks"]

task_list = [task["task_key"] for task in tasks]
task_names = ",".join(task_list)


job_tasks = create_tasks(tasks)

import time

# Create job with tasks and configuration from the [config_file].
created_job = w.jobs.create(
    name=f"{job['name']}-{time.time_ns()}",
    tags=job["tags"],
    git_source=jobs.GitSource(
        git_url=job["git_url"],
        git_provider=jobs.GitProvider.gitHub,
        git_branch=job["git_branch"],
    ),
    job_clusters=[
        jobs.JobCluster(
            job_cluster_key="default_cluster",
            new_cluster=compute.ClusterSpec(
                spark_version=w.clusters.select_spark_version(long_term_support=True),
                node_type_id="Standard_DS3_v2",
                num_workers=1,
            ),
        )
    ],
    tasks=job_tasks,
)

# Run the newly created job.
run_by_id = w.jobs.run_now(job_id=created_job.job_id).result()

# Cleanup -> Delete the job on completion.
w.jobs.delete(job_id=created_job.job_id)

