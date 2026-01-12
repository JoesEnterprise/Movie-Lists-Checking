import sqlite3
import argparse
import os
from typing import List, Tuple, Optional

# Optional .env support (install python-dotenv to use)
try:
    from dotenv import load_dotenv
    _HAVE_DOTENV = True
except Exception:
    _HAVE_DOTENV = False

if _HAVE_DOTENV:
    load_dotenv()

# Path to your SQLite database file (change as needed)
DB_PATH = 'nama_database_Anda.db'

EXAMPLE_QUERIES: List[Tuple[str, str]] = [
    ("Movies with large budget and specific director",
     "SELECT * FROM movies WHERE budget > 30000000 AND director_id = 5417"),

    ("Recent or highly-rated movies",
     "SELECT * FROM movies WHERE release_date > '2015-01-01' OR vote_average > 8"),

    ("Movies with non-zero budget",
     "SELECT * FROM movies WHERE budget != 0"),

    ("Movies released in Jan 2015",
     "SELECT * FROM movies WHERE release_date BETWEEN '2015-01-01' AND '2015-02-01'"),

    ("Titles starting with 'Harry Potter'",
     "SELECT * FROM movies WHERE title LIKE 'Harry Potter%'") ,

    ("Movies mentioning 'programmer' in tagline",
     "SELECT * FROM movies WHERE tagline LIKE '%programmer%'") ,

    ("Movies with director in a list",
     "SELECT * FROM movies WHERE director_id IN (4765, 5417)"),

    ("Movies that have a tagline",
     "SELECT * FROM movies WHERE tagline IS NOT NULL"),

    ("First 10 movies",
     "SELECT * FROM movies LIMIT 10"),

    ("Ten movies starting from row 100",
     "SELECT * FROM movies LIMIT 10 OFFSET 100"),
]


def run_query(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> List[sqlite3.Row]:
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    return rows


def get_discord_token(cli_token: Optional[str] = None) -> Optional[str]:
    """Return the Discord bot token.

    Priority: --discord-token CLI override > DISCORD_TOKEN env var
    Do NOT print the token to logs. Use this to pass the token to your bot library.
    """
    if cli_token:
        return cli_token
    return os.getenv('DISCORD_TOKEN')


def print_rows(rows: List[sqlite3.Row]) -> None:
    if not rows:
        print("(no rows)")
        return

    # Print column headers
    headers = rows[0].keys() if hasattr(rows[0], 'keys') else [f"col{i}" for i in range(len(rows[0]))]
    print('\t'.join(headers))

    for r in rows:
        print('\t'.join(str(r[h]) for h in headers))


def main():
    parser = argparse.ArgumentParser(description='Run example queries against a SQLite movies DB')
    parser.add_argument('--db', default=DB_PATH, help='Path to SQLite database file')
    parser.add_argument('--list', action='store_true', help='List example queries')
    parser.add_argument('--run', type=int, help='Run a single example query by its 1-based index')

    # Discord token helpers
    parser.add_argument('--discord-token', help='Discord bot token (not recommended on command line). Prefer setting DISCORD_TOKEN env var')
    parser.add_argument('--check-discord', action='store_true', help="Check presence of a Discord token (won't print it)")

    args = parser.parse_args()

    # If user just wants to check Discord token availability, do that and exit
    if args.check_discord:
        token = get_discord_token(args.discord_token)
        if token:
            print('Discord token is available via environment or --discord-token.')
        else:
            print('No Discord token found. Set DISCORD_TOKEN env var or pass --discord-token.')
        return

    # If a token was provided on the command line, warn briefly (do not print the token)
    if args.discord_token:
        print('Warning: passing secrets via command line is insecure; prefer DISCORD_TOKEN env var.')

    try:
        with sqlite3.connect(args.db) as conn:
            conn.row_factory = sqlite3.Row

            if args.list:
                for i, (name, sql) in enumerate(EXAMPLE_QUERIES, start=1):
                    print(f"{i}. {name}: {sql}")
                return

            if args.run is not None:
                idx = args.run - 1
                if not (0 <= idx < len(EXAMPLE_QUERIES)):
                    print("Invalid query index. Use --list to see available queries.")
                    return
                name, sql = EXAMPLE_QUERIES[idx]
                print(f"Running [{args.run}] {name}...\nSQL: {sql}\n")
                rows = run_query(conn, sql)
                print_rows(rows)
                return

            # Default: run all examples (prints a header and first 10 rows each)
            for i, (name, sql) in enumerate(EXAMPLE_QUERIES, start=1):
                print('=' * 80)
                print(f"{i}. {name}")
                try:
                    rows = run_query(conn, sql)
                except sqlite3.Error as e:
                    print(f"SQL error: {e}")
                    continue
                print_rows(rows[:10])

    except sqlite3.Error as e:
        print(f"Could not open database '{args.db}': {e}")


if __name__ == '__main__':
    main()