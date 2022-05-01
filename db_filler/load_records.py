import re
import sqlite3
from typing import List

import requests
import spacy
from tqdm import tqdm

nlp = spacy.load("en_core_web_trf")


def fill_id(string: str, id: int):
    return string.replace("{id}", str(id))


def is_technical(quote: str):
    return re.match(r"\([^\(\)]+\)", quote) is not None


def split_quote(quote: str) -> List[str]:
    quote = re.sub(r'[."\-)(,;?!:\n]', '', quote).lower()
    doc = nlp(quote)
    words = [token.lemma_ for token in doc]
    return words


def load_records(config, is_test):
    print('NLTK resources downloaded')

    if is_test:
        conn = sqlite3.connect(config["TEST"]["DbName"])
        n_of_records = int(config["TEST"]["NumOfVideos"])
    else:
        conn = sqlite3.connect(config["DEFAULT"]["DbName"])
        n_of_records = int(config["DEFAULT"]["NumOfVideos"])

    curs = conn.cursor()
    pbar = tqdm(range(1, n_of_records + 1))
    success = 0
    fail = 0
    for rec_id in pbar:
        pbar.set_postfix({"Success": success, "Fail": fail})
        talk_url = fill_id(config["DEFAULT"]["TedTalkUrl"], rec_id)
        talk_page_response = requests.get(talk_url)
        if not talk_page_response.ok:
            fail += 1
            continue
        talk_page_source = talk_page_response.text
        file_url = re.search(r"https:\/\/py\.tedcdn\.com\S*\.mp4", talk_page_source).group(0)

        curs.execute(
            """
            INSERT INTO videos VALUES (?)
            """,
            [file_url],
        )

        video_id = curs.lastrowid
        subtitles_url = fill_id(config["DEFAULT"]["TedSubUrl"], rec_id)
        subtitle_page_response = requests.get(subtitles_url)
        if not subtitle_page_response.ok:
            curs.execute("rollback")
            fail += 1
            continue

        subtitles = subtitle_page_response.json()["captions"]
        for quote in subtitles:
            if is_technical(quote['content']):
                continue
            curs.execute(
                """
                INSERT INTO subtitles VALUES (?, ?, ?)
                """,
                [quote["content"], quote["startTime"], quote["startTime"] + quote["duration"]],
            )
            quote_id = curs.lastrowid
            curs.execute(
                """
                INSERT INTO video_to_quote VALUES (?, ?)
                """,
                [video_id, quote_id],
            )
            for word in split_quote(quote['content']):
                curs.execute(
                    """
                    INSERT OR IGNORE INTO words VALUES (?)    
                    """,
                    [word],
                )

                curs.execute(
                    """
                    SELECT ROWID FROM words where word=?
                    """
                    , [word])
                word_id = curs.fetchone()[0]
                curs.execute(
                    """
                    INSERT OR IGNORE INTO word_to_quote VALUES (?, ?)
                    """,
                    [word_id, quote_id],
                )

        conn.commit()
        success += 1
        pbar.set_postfix({"Success": success, "Fail": fail})

    curs.execute(
        """
        SELECT * FROM words
        """
    )
    print(curs.fetchmany(10))
    conn.close()
