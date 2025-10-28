from logging import exception, info
from threading import Event, Lock

from .shared import GROUP, get_stream_key, get_valkey


class LocalMonitor:
    def __init__(
        self,
        valkey_uri: str,
        job_type: str,
        local_task_type: str,
        consumer: str,
        running_task_ids: dict[int, list[str]],
        running_task_ids_lock: Lock,
    ):
        self.valkey_uri = valkey_uri
        self.job_type = job_type
        self.local_task_type = local_task_type
        self.consumer = consumer
        self.running_task_ids = running_task_ids
        self.running_task_ids_lock = running_task_ids_lock

    def reset_pel_idle_times(self):
        with self.running_task_ids_lock:
            if not self.running_task_ids:
                return
            for k, v in list(self.running_task_ids.items()):
                if not v:
                    self.running_task_ids.pop(k)
            if not self.running_task_ids:
                return
            cli = get_valkey(self.valkey_uri)
            pipe = cli.pipeline()
            for job_id, ids in self.running_task_ids.items():
                stream = get_stream_key(self.job_type, job_id, self.local_task_type)
                info(f"Resetting entries in {stream}: {ids}")
                pipe.pipeline_execute_command(
                    "XCLAIM", stream, GROUP, self.consumer, "0", *ids, "JUSTID"
                )
            pipe.execute()

    def monitor_jobs(self):
        self.reset_pel_idle_times()

    def start(self, exit_flag: Event):
        info("Starting local jobs monitor")
        while not exit_flag.wait(timeout=5):
            try:
                self.monitor_jobs()
            except Exception:
                exception("Error in local jobs monitor:")
        info("Exiting local jobs monitor")
