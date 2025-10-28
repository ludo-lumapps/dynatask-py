from logging import info
from threading import Event, Lock, Thread
from typing import Callable
from signal import signal, SIGINT, SIGTERM

from .shared import WorkConf
from .global_monitor import FinishedJobInfo, GlobalMonitor
from .local_monitor import LocalMonitor
from .worker import WorkDispatcher


def start_work(
    conf: WorkConf,
    job_is_done_handler: Callable[[FinishedJobInfo], None] | None,
    task_handler: Callable[[bytes], None],
    exit_flag: Event | None = None,
):
    exit_flag = exit_flag or Event()

    def time_to_exit(signal, frame):
        info("Interrupted...")
        exit_flag.set()

    info(f"Starting [{conf.job_type}] processing [{conf.local_task_type}] tasks")
    signal(SIGINT, time_to_exit)
    signal(SIGTERM, time_to_exit)  # K8s sends SIGTERM for graceful pod shutdown

    global_monitor = GlobalMonitor(conf, job_is_done_handler)
    global_monitor_thread = Thread(
        target=global_monitor.start,
        kwargs={"exit_flag": exit_flag},
        name=f"{conf.consumer}-global-monitor",
    )
    global_monitor_thread.start()

    running_task_ids: dict[int, list[str]] = {}
    running_task_ids_lock = Lock()
    local_monitor = LocalMonitor(
        conf.valkey_uri,
        conf.job_type,
        conf.local_task_type,
        conf.consumer,
        running_task_ids,
        running_task_ids_lock,
    )
    local_monitor_thread = Thread(
        target=local_monitor.start,
        kwargs={"exit_flag": exit_flag},
        name=f"{conf.consumer}-local-monitor",
    )
    local_monitor_thread.start()

    work_dispatcher = WorkDispatcher(
        conf, running_task_ids, running_task_ids_lock, task_handler
    )
    work_dispatcher.dispatch_work(exit_flag)

    local_monitor_thread.join()
    global_monitor_thread.join()
    info("Exiting...")
