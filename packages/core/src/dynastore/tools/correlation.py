from contextvars import ContextVar, Token

_correlation_id_var: ContextVar[str | None] = ContextVar("correlation_id", default=None)

_INTERNAL_KEY = "_request_correlation_id"


def get_correlation_id() -> str | None:
    return _correlation_id_var.get()


def set_correlation_id(cid: str) -> Token:
    return _correlation_id_var.set(cid)
