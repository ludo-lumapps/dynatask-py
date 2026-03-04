from dataclasses import dataclass
from logging import exception, info
from typing import Any, Callable
from threading import Event

from valkey import WatchError

from .shared import (
    MyValkey,
    WorkConf,
    get_active_job_ids_key,
    get_job_stats,
    get_stopping_job_ids_key,
    get_stream_key,
    get_stream_watch_key,
    get_valkey,
)


@dataclass
class FinishedJobInfo:
    id: int
    started_at: str
    tasks_added: int
    tasks_read: int
    tasks_done_ok: int
    tasks_done_err: int


class GlobalMonitor:
    def __init__(
        self,
        conf: WorkConf,
        job_is_done_handler: Callable[[FinishedJobInfo], None] | None,
    ):
        self.conf = conf
        self.job_is_done_handler = job_is_done_handler

    def job_finished(self, cli: MyValkey, job_id: int) -> None:
        job_type = self.conf.job_type
        job_stats = get_job_stats(cli, job_type, job_id)
        streams = [get_stream_key(job_type, job_id, tt) for tt in self.conf.task_types]
        pipe = cli.pipeline()
        pipe.pipeline_execute_command("SREM", get_active_job_ids_key(job_type), job_id)
        pipe.pipeline_execute_command(
            "SREM", get_stopping_job_ids_key(job_type), job_id
        )
        for stream in streams:
            watch_key = get_stream_watch_key(stream)
            pipe.pipeline_execute_command("DEL", watch_key)
            pipe.pipeline_execute_command("DEL", stream)
        pipe.execute()
        if f := self.job_is_done_handler:
            job_info = FinishedJobInfo(
                job_id,
                job_stats.started_at,
                job_stats.tasks_added,
                job_stats.tasks_read,
                job_stats.tasks_done_ok,
                job_stats.tasks_done_err,
            )
            f(job_info)

    def job_is_done(self, cli: MyValkey, job_id: int) -> bool:
        job_type = self.conf.job_type
        streams = [get_stream_key(job_type, job_id, tt) for tt in self.conf.task_types]
        pipe = cli.pipeline()
        pipe.execute_command("WATCH", *streams)
        pipe.multi()
        pipe.pipeline_execute_command(
            "SISMEMBER", get_stopping_job_ids_key(job_type), job_id
        )
        for stream in streams:
            pipe.pipeline_execute_command("XINFO", "GROUPS", stream)
        try:
            res = pipe.execute()
        except WatchError:
            return False
        """ [
                1,
                [
                    [
                        b'name', b'group-1',
                        b'consumers', 1,
                        b'pending', 1,
                        b'last-delivered-id', b'1756976941174-0',
                        b'entries-read', 1,
                        b'lag', 1
                    ]
                ]
            ] """
        is_stopping = res[0] != 0

        def pairs_lst_to_dict(pairs_in_lst: list[Any]) -> dict[str, Any]:
            ret = {}
            for pair in zip(pairs_in_lst[::2], pairs_in_lst[1::2]):
                ret[pair[0].decode()] = pair[1]
            return ret

        # A job is finished when all its streams are empty
        for group in [grp for grps in res[1:] for grp in grps]:
            group_d = pairs_lst_to_dict(group)
            pending = group_d.get("pending") or 0
            if is_stopping:
                if pending > 0:
                    return False
            elif (group_d.get("lag") or 0) > 0 or pending > 0:
                return False
        return True

    def monitor_job(self, cli: MyValkey, job_id: int) -> None:
        info(f"Monitoring job {job_id}")
        if self.job_is_done(cli, job_id):
            info(f"Job {job_id} has nothing left to do, time to end it")
            self.job_finished(cli, job_id)
        else:
            info(f"Job {job_id} still has tasks, so leave it running")

    def monitor_jobs(self) -> None:
        job_type = self.conf.job_type
        throttle_key = f"{job_type}-jobs-monitor-throttle"
        cli = get_valkey(self.conf.valkey_uri)
        if cli.cmd("SET", throttle_key, "1", "GET", "EX", "3", "NX"):
            return
        job_ids: list[str] = cli.cmd("SMEMBERS", get_active_job_ids_key(job_type))
        for job_id in job_ids:
            cli.cmd("SETEX", throttle_key, "3", "1")
            self.monitor_job(cli, int(job_id))

    def start(self, exit_flag: Event):
        info("Starting global jobs monitor")
        while not exit_flag.wait(timeout=2):
            try:
                self.monitor_jobs()
            except Exception:
                exception("Error in global_monitor:")
        info("Exiting global jobs monitor")
