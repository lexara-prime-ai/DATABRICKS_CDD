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

# Create job with tasks and configuration from the [config_file].
created_job = 