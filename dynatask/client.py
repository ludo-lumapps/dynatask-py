from enum import Enum
from logging import info
from time import sleep
from datetime import datetime, timezone
from zlib import compress

from valkey.client import Pipeline

from .shared import (
    GROUP,
    TASK_LOCAL,
    JobStats,
    TaskContext,
    get_active_job_ids_key,
    get_job_stats,
    get_job_stats_key,
    get_job_spans_key,
    get_stopping_job_ids_key,
    get_stream_key,
    get_valkey,
)

MAX_PAYLOAD_SIZE = 10_000


def prep_task_params(data: bytes) -> bytes:
    ret = compress(data)
    if (s := len(ret)) > MAX_PAYLOAD_SIZE:
        raise JobClientUserError(f"Task payload {s} > {MAX_PAYLOAD_SIZE}B max")
    return ret


def add_task_to_pipeline(
    job_type: str,
    job_id: int,
    stream: str,
    stats_key: str,
    data: bytes,
    spans: list[str],
    pipe: Pipeline,
):
    spans_v = prep_task_params("|".join(spans).encode())
    data_v = prep_task_params(data)
    if spans:
        spans_key = get_job_spans_key(job_type, job_id)
        for span in spans:
            info(f"Increasing span {span} by 1")
            pipe.pipeline_execute_command("HINCRBY", spans_key, span, "1")
    pipe.pipeline_execute_command("HINCRBY", stats_key, "added", "1")
    pipe.pipeline_execute_command("XADD", stream, "*", "data", data_v, "spans", spans_v)
    return len(data_v)


class JobClientUnhandledError(Exception):
    def __init__(self, message=None):
        super().__init__(message)


class JobClientUserError(Exception):
    def __init__(self, message=None):
        super().__init__(message)


class Client:
    def __init__(self, valkey_uri: str, job_type: str, task_types: list[str]):
        self.valkey_uri = valkey_uri
        self.job_type = job_type
        self.task_types = task_types
        self.active_job_ids_key = get_active_job_ids_key(job_type)
        self.stopping_job_ids_key = get_stopping_job_ids_key(job_type)

    def start_job(self, job_id: int, task_type: str, data: bytes):
        job_type = self.job_type
        # 1- grab a lock
        lock_key = f"stream-creation|{job_type}|{job_id}"
        cli = get_valkey(self.valkey_uri)
        if cli.cmd("SET", lock_key, "1", "GET", "EX", "30", "NX"):
            raise JobClientUserError(f"Job {job_id} already running")
        # 2- check if job exists
        if cli.cmd("SISMEMBER", self.active_job_ids_key, job_id):
            cli.cmd("DEL", lock_key)
            raise JobClientUserError(f"Job {job_id} already running")
        stats_key = get_job_stats_key(job_type, job_id)
        pipe = cli.pipeline()
        pipe.multi()
        pipe.pipeline_execute_command("DEL", stats_key)
        pipe.pipeline_execute_command("SADD", self.active_job_ids_key, job_id)
        pipe.pipeline_execute_command("SREM", self.stopping_job_ids_key, job_id)
        for tt in self.task_types:
            stream = get_stream_key(job_type, job_id, tt)
            pipe.pipeline_execute_command(
                "XGROUP", "CREATE", stream, GROUP, "0", "MKSTREAM"
            )
        started_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        pipe.pipeline_execute_command("HSET", stats_key, "started_at", started_at)
        stream = get_stream_key(job_type, job_id, task_type)
        pipe.pipeline_execute_command("DEL", lock_key)
        pl_len = add_task_to_pipeline(
            job_type, job_id, stream, stats_key, data, [], pipe
        )
        res = pipe.execute()
        entry_id = res[-1]  # XADD ... -> '1756815635488-0'
        info(
            f"Added first task {entry_id} "
            f"to stream {stream} of job {job_id}, size {pl_len}"
        )

    def stop_job(self, job_id: int) -> bool:
        cli = get_valkey(self.valkey_uri)
        if cli.cmd("SISMEMBER", self.active_job_ids_key, job_id):
            cli.cmd("SADD", self.stopping_job_ids_key, job_id)
            return True
        else:
            return False

    def job_is_running(self, job_id: int) -> bool:
        cli = get_valkey(self.valkey_uri)
        return bool(cli.cmd("SISMEMBER", self.active_job_ids_key, job_id))

    def get_job_stats(self, job_id: int) -> JobStats:
        cli = get_valkey(self.valkey_uri)
        return get_job_stats(cli, self.job_type, job_id)


class InTaskContext:
    class TrackedTasksState(Enum):
        JobStopping = 0
        AllFinished = 1
        Pending = 2

    """
    get task context from a thread local variable
    that variable is kept in .shared
    """

    @staticmethod
    def add_task(task_type: str, data: bytes, track: bool) -> None:
        task_context: TaskContext = TASK_LOCAL.task_context
        job_type = task_context.job_type
        job_id = task_context.job_id
        stream = get_stream_key(job_type, job_id, task_type)
        stats_key = get_job_stats_key(job_type, job_id)
        cli = get_valkey(task_context.valkey_uri)
        pipe = cli.pipeline()
        parent_spans = task_context.spans
        sub_task_spans = []
        if parent_spans:
            sub_task_spans.extend(parent_spans)
        if track:
            sub_task_spans.append(task_context.task_id)
        pl_len = add_task_to_pipeline(
            job_type, job_id, stream, stats_key, data, sub_task_spans, pipe
        )
        res = pipe.execute()
        entry_id = res[-1]  # XADD ... -> '1756815635488-0'
        info(f"Added {entry_id} to stream {stream}, size {pl_len}")

    @staticmethod
    def job_is_stopping() -> bool:
        task_context: TaskContext = TASK_LOCAL.task_context
        cli = get_valkey(task_context.valkey_uri)
        res = cli.cmd(
            "SISMEMBER",
            get_stopping_job_ids_key(task_context.job_type),
            task_context.job_id,
        )  # return 0 or 1, as an int
        return bool(res)

    @staticmethod
    def get_tracked_tasks_state() -> tuple[TrackedTasksState, int | None]:
        if InTaskContext.job_is_stopping():
            # // tracking if job is stopping
            return InTaskContext.TrackedTasksState.JobStopping, None
        task_context: TaskContext = TASK_LOCAL.task_context
        cli = get_valkey(task_context.valkey_uri)
        res = cli.cmd(
            "HGET",
            get_job_spans_key(task_context.job_type, task_context.job_id),
            task_context.task_id,
        )  # returns a string, or None
        if res:
            res_int = int(res)
            if res_int == 0:
                return InTaskContext.TrackedTasksState.AllFinished, None
            else:
                return InTaskContext.TrackedTasksState.Pending, res_int
        else:
            return InTaskContext.TrackedTasksState.AllFinished, None

    @staticmethod
    def wait_until_tracked_tasks_done() -> None:
        while True:
            tasks_state, pending = InTaskContext.get_tracked_tasks_state()
            match tasks_state:
                case InTaskContext.TrackedTasksState.JobStopping:
                    info("Job is stoppping, not waiting for tracked tasks to finish")
                    return
                case InTaskContext.TrackedTasksState.AllFinished:
                    info("tracked subtasks have all finished")
                    return
                case InTaskContext.TrackedTasksState.Pending:
                    info(f"{pending} tracked subtasks are still pending, sleeping 3s")
            sleep(2)
