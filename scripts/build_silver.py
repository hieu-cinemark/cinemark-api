"""Rebuilds the lake's silver tables from bronze (see app/lake/silver.py).

    .venv/bin/python -m scripts.build_silver            # write to R2 silver/
    .venv/bin/python -m scripts.build_silver --local    # write to ./.lake-local/silver to inspect first
"""

from __future__ import annotations

import argparse
import time

from app.lake.silver import SILVER, build_posts, connect

LOCAL_OUT = ".lake-local/silver"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--local", action="store_true", help=f"write to {LOCAL_OUT} instead of R2")
    args = parser.parse_args()

    out = LOCAL_OUT if args.local else SILVER
    started = time.perf_counter()
    con = connect()
    counts = build_posts(con, out=out)
    print(f"silver posts -> {out}: {counts} in {time.perf_counter() - started:.1f}s")
    con.sql(
        """SELECT platform, count(*) AS posts, count(decision) AS with_decision,
                  count(*) FILTER (WHERE decision = 'dropped') AS dropped,
                  min(posted_at)::DATE AS oldest, max(scraped_at)::DATE AS last_crawl
           FROM posts GROUP BY platform ORDER BY platform"""
    ).show()


if __name__ == "__main__":
    main()
