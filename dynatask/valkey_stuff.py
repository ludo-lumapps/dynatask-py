from dataclasses import dataclass
from typing import Any, Self

from valkey import ConnectionPool, Valkey, ResponseError

MAX_ELAPSED_MS_S = "60000"
LOCK_MAX_TIME_IN_SECONDS = 60


@dataclass
class PendingIdledEntry:
    id: str
    times_delivered: int


@dataclass
class Entry:
    id: str
    data: dict[str, bytes]


def pairs_lst_to_dict(pairs_in_lst: list[Any]) -> dict[str, str]:
    ret = {}
    for pair in zip(pairs_in_lst[::2], pairs_in_lst[1::2]):
        ret[pair[0].decode()] = pair[1].decode()
    return ret


class MyValkey(Valkey):
    @classmethod
    def from_url(cls, url: str, **kwargs) -> Self:
        connection_pool = ConnectionPool.from_url(url, **kwargs)
        client = cls(
            connection_pool=connection_pool, protocol=3, single_connection_client=False
        )
        client.auto_close_connection_pool = True
        client.response_callbacks["XPENDING"] = lambda v: v
        client.response_callbacks["XCLAIM"] = lambda v: v
        client.response_callbacks["HGET"] = lambda v: v.decode()
        client.response_callbacks["SMEMBERS"] = lambda v: [i.decode() for i in v]
        client.response_callbacks["SRANDMEMBER"] = lambda v: [i.decode() for i in v]
        client.response_callbacks["HGETALL"] = pairs_lst_to_dict
        client.response_callbacks["XADD"] = lambda v: v.decode()
        return client

    def cmd(self, *parts) -> Any:
        return self.execute_command(*parts)

    def get_pending_entries(
        self, stream: str, group: str, count: str
    ) -> list[PendingIdledEntry] | None:
        # [[b'1756972372093-0', b'consumer-1', 11985, 1], ...]
        resp = self.cmd(
            "XPENDING", stream, group, "IDLE", MAX_ELAPSED_MS_S, "-", "+", count
        )
        try:
            return [PendingIdledEntry(e[0].decode(), e[3]) for e in resp]
        except ResponseError as err:
            if "NOGROUP" in str(err):
                return None
            raise

    def claim_entries(self, stream: str, group: str, consumer: str, ids: list[str]):
        # [[b'1756797922859-0', [b'key1', b'val1', b'key2', b'val2']],
        #       [b'1756798215818-0', [b'key1', b'val1', b'key2', b'val2']]]
        try:
            resp = self.cmd("XCLAIM", stream, group, consumer, MAX_ELAPSED_MS_S, *ids)
        except ResponseError as err:
            if "NOGROUP" in str(err):
                return None
            raise
        return [Entry(e[0].decode(), {e[1][0].decode(): e[1][1]}) for e in resp]
