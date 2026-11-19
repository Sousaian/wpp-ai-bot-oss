"""Shared destructive-suite guard for disposable MySQL schemas."""
import os

from sqlalchemy import text


async def assert_disposable_database(test_case, engine) -> None:
    marker = os.environ.get("WPP_DISPOSABLE_TEST_DATABASE_MARKER")
    test_case.assertTrue(marker, "WPP_DISPOSABLE_TEST_DATABASE_MARKER is required")
    async with engine.connect() as connection:
        current_database = (await connection.execute(text("SELECT DATABASE()"))).scalar_one()
    test_case.assertEqual(current_database, marker)
