import sqlite3
from config import DATABASE

QUERIES = {
    'last_release_per_director': '''
        SELECT d.name, MAX(m.release_date) AS last_release_date
        FROM directors d
        INNER JOIN movies m ON m.director_id = d.id
        GROUP BY d.id, d.name
        ORDER BY last_release_date DESC;
    ''',
    'total_budget_per_director': '''
        SELECT d.name, SUM(m.budget) AS total_budget
        FROM directors d
        INNER JOIN movies m ON m.director_id = d.id
        GROUP BY d.id, d.name
        ORDER BY total_budget DESC;
    ''',
    'num_films_per_director': '''
        SELECT d.name, COUNT(m.id) AS num_films
        FROM directors d
        INNER JOIN movies m ON m.director_id = d.id
        GROUP BY d.id, d.name
        ORDER BY num_films DESC;
    ''',
    'genre_count_per_movie': '''
        SELECT m.title, COUNT(mg.genre_id) AS genre_count
        FROM movies m
        LEFT JOIN movie_genres mg ON mg.movie_id = m.id
        GROUP BY m.id, m.title
        ORDER BY genre_count DESC, m.title;
    ''',
    'distinct_genres_per_director': '''
        SELECT d.name, COUNT(DISTINCT mg.genre_id) AS distinct_genres_count
        FROM directors d
        LEFT JOIN movies m ON m.director_id = d.id
        LEFT JOIN movie_genres mg ON mg.movie_id = m.id
        GROUP BY d.id, d.name
        ORDER BY distinct_genres_count DESC;
    '''
}

SAMPLE_DATA = {
    'directors': [
        (1, 'Alice'),
        (2, 'Bob'),
        (3, 'Charlie')
    ],
    'movies': [
        (1, 'Alpha', 1, '2020-05-01', 1000000),
        (2, 'Beta', 1, '2022-09-10', 1500000),
        (3, 'Gamma', 2, '2021-03-20', 750000),
        (4, 'Delta', 3, '2023-01-15', 500000),
        (5, 'Epsilon', 2, '2019-07-07', 300000)
    ],
    'genres': [
        (1, 'Drama'),
        (2, 'Action'),
        (3, 'Sci-Fi'),
        (4, 'Comedy')
    ],
    'movie_genres': [
        (1, 1),
        (1, 3),
        (2, 2),
        (3, 1),
        (4, 4),
        (2, 1),
        (5, 2)
    ]
}


def initialize_db(db_path):
    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS directors (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS movies (
                id INTEGER PRIMARY KEY,
                title TEXT,
                director_id INTEGER,
                release_date TEXT,
                budget INTEGER,
                FOREIGN KEY(director_id) REFERENCES directors(id)
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS genres (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS movie_genres (
                movie_id INTEGER,
                genre_id INTEGER,
                FOREIGN KEY(movie_id) REFERENCES movies(id),
                FOREIGN KEY(genre_id) REFERENCES genres(id)
            )
        ''')

        # Insert sample data using INSERT OR IGNORE to allow re-running
        conn.executemany('INSERT OR IGNORE INTO directors(id, name) VALUES (?, ?)', SAMPLE_DATA['directors'])
        conn.executemany('INSERT OR IGNORE INTO movies(id, title, director_id, release_date, budget) VALUES (?, ?, ?, ?, ?)', SAMPLE_DATA['movies'])
        conn.executemany('INSERT OR IGNORE INTO genres(id, name) VALUES (?, ?)', SAMPLE_DATA['genres'])
        conn.executemany('INSERT OR IGNORE INTO movie_genres(movie_id, genre_id) VALUES (?, ?)', SAMPLE_DATA['movie_genres'])
        conn.commit()
    conn.close()


def run_queries(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    for name, q in QUERIES.items():
        print('\n----', name.replace('_', ' ').upper(), '----')
        cur.execute(q)
        rows = cur.fetchall()
        for row in rows:
            print(row)
    conn.close()


if __name__ == '__main__':
    print('Using database:', DATABASE)
    initialize_db(DATABASE)
    print('Sample data inserted (or already present). Running queries...')
    run_queries(DATABASE)
    print('\nDone.')
