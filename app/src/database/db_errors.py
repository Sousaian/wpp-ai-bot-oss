"""MySQL error classification shared by repositories.

After a deadlock (1213) InnoDB rolls back the whole transaction, which also
discards every savepoint; the following ``ROLLBACK TO SAVEPOINT`` then fails
with 1305 and carries the original error only as its context. A lock wait
timeout (1205) is treated the same way. None of these may be absorbed by a
savepoint fallback: the caller must restart the complete transaction.
"""

from __future__ import annotations

from sqlalchemy.exc import DBAPIError


TRANSACTION_LOST_CODES = frozenset({1205, 1213, 1305})


def mysql_error_codes(exc: BaseException) -> tuple[int, ...]:
    """Every MySQL error code in the wrapper, cause and context chain."""

    codes: list[int] = []
    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        args = getattr(current, "args", ())
        if args and isinstance(args[0], int):
            codes.append(args[0])
        for linked in (
            getattr(current, "orig", None),
            current.__cause__,
            current.__context__,
        ):
            if isinstance(linked, BaseException):
                pending.append(linked)
    return tuple(codes)


def transaction_lost(exc: BaseException) -> bool:
    return any(code in TRANSACTION_LOST_CODES for code in mysql_error_codes(exc))


def transaction_lost_dbapi_error(exc: BaseException) -> DBAPIError | None:
    """Return the chained SQLAlchemy error that proves transaction loss."""

    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, DBAPIError) and transaction_lost(current):
            return current
        for linked in (
            getattr(current, "orig", None),
            current.__cause__,
            current.__context__,
        ):
            if isinstance(linked, BaseException):
                pending.append(linked)
    return None
