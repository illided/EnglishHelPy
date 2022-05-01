import sqlite3


def initialize_db(config, is_test):
    if is_test:
        conn = sqlite3.connect(config["TEST"]["DbName"])
    else:
        conn = sqlite3.connect(config["DEFAULT"]["DbName"])
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS words (
            word VARCHAR UNIQUE 
        )
    """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS subtitles (
            quote VARCHAR,
            start_time INTEGER,
            end_time INTEGER
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS word_to_quote (
            word_id INTEGER,
            quote_id INTEGER,
            CONSTRAINT unq UNIQUE (word_id, quote_id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS videos (
            link TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS video_to_quote (
            video_id INTEGER,
            quote_id INTEGER,
            CONSTRAINT unq UNIQUE (video_id, quote_id)
        )
        """
    )

    conn.commit()
    conn.close()
