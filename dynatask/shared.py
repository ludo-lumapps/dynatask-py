from dataclasses import dataclass
from os import getenv
from threading import local

from .valkey_stuff import MyValkey

GROUP = "group-1"
TASK_LOCAL = local()


def get_valkey(uri: str):
    return MyValkey.from_url(uri)


class WorkConf:
    def __init__(
        self,
        valkey_uri: str,
        job_type: str,
        local_task_type: str,
        task_types: list[str] | None = None,
    ):
        self.valkey_uri = valkey_uri
        self.job_type = job_type
        self.local_task_type = local_task_type
        self.task_types = task_types or [local_task_type]
        self.consumer = getenv("HOSTNAME") or "consumer-1"
        self.max_task_attempts = 1
        self.max_tasks_per_job = 10
        self.thread_count = 10


def get_stream_key(job_type: str, job_id: int, task_type: str) -> str:
    return f"{job_type}|{job_id}|{task_type}|stream"


def get_stream_watch_key(stream: str) -> str:
    return f"{stream}|watch"


def get_active_job_ids_key(job_type: str) -> str:
    return f"{job_type}|jobs"


def get_stopping_job_ids_key(job_type: str) -> str:
    return f"{job_type}|stopping_jobs"


def get_job_stats_key(job_type: str, job_id: int) -> str:
    return f"{job_type}|{job_id}|stats"


def get_job_spans_key(job_type: str, job_id: int) -> str:
    return f"{job_type}|{job_id}|spans"


@dataclass
class JobStats:
    started_at: str
    tasks_added: int
    tasks_read: int
    tasks_done_ok: int
    tasks_done_err: int


def get_job_stats(cli: MyValkey, job_type: str, job_id: int) -> JobStats:
    if not cli.cmd("SISMEMBER", get_active_job_ids_key(job_type), job_id):
        return JobStats("", 0, 0, 0, 0)
    stats_key = get_job_stats_key(job_type, job_id)
    st: dict[str, str] = cli.cmd("HGETALL", stats_key)
    if not st:
        return JobStats("", 0, 0, 0, 0)
    return JobStats(
        st.get("started_at") or "",
        int(st.get("added") or 0),
        int(st.get("read") or 0),
        int(st.get("done_ok") or 0),
        int(st.get("done_err") or 0),
    )


@dataclass
class TaskContext:
    valkey_uri: str
    job_type: str
    job_id: int
    task_id: str
    task_type: str
    spans: list[str] | None
