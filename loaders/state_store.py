from sqlalchemy import text
from .config import engine  # ou get_engine()

SOURCE = "sales"

def get_last_sales_id() -> int | None:
    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT last_sales_id
            FROM meta.ingestion_state
            WHERE source_name = :source
            LIMIT 1
        """), {"source": SOURCE}).first()
        return int(row[0]) if row and row[0] is not None else None

def set_last_sales_id(last_id: int, note: str | None = None) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO meta.ingestion_state (source_name, last_sales_id, last_run_ts, note)
            VALUES (:source, :last_id, now(), :note)
            ON CONFLICT (source_name) DO UPDATE
            SET last_sales_id = EXCLUDED.last_sales_id,
                last_run_ts   = now(),
                note          = COALESCE(EXCLUDED.note, meta.ingestion_state.note)
        """), {"source": SOURCE, "last_id": last_id, "note": note})
