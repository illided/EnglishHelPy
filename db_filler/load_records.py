import json
import re
import sqlite3
from typing import List, Optional

import requests
import spacy
from tqdm import tqdm


def fill_id(string: str, id: int):
    return string.replace("{id}", str(id))


class TableLoader:
    TECHNICAL_REGEX = re.compile(r"\([^\(\)]+\)")
    JUNK_SYMBOLS_REGEX = re.compile(r'[."\-)(,;?!:]')
    FILE_URL_REGEX = re.compile(r"https:\/\/py\.tedcdn\.com\S*\.mp4")

    def __init__(self, config, is_test):
        self.config = config
        self.is_test = is_test
        self.nlp = spacy.load("en_core_web_trf")

        self.conn = None
        if is_test:
            self.n_of_records = int(self.config["TEST"]["NumOfVideos"])
        else:
            self.n_of_records = int(config["DEFAULT"]["NumOfVideos"])

    def is_technical(self, quote: dict) -> bool:
        return re.match(self.TECHNICAL_REGEX, quote["content"]) is not None

    def split_quote(self, quote: str) -> List[str]:
        quote = re.sub(self.JUNK_SYMBOLS_REGEX, "", quote).lower()
        doc = self.nlp(quote)
        words = [token.lemma_ for token in doc]
        return words

    def create_conn(self) -> sqlite3.Connection:
        if self.is_test:
            return sqlite3.connect(self.config["TEST"]["DbName"])
        else:
            return sqlite3.connect(self.config["DEFAULT"]["DbName"])

    def load_page_source(self, url: str) -> Optional[str]:
        response = requests.get(url)
        if not response.ok:
            return None
        return response.text

    def extract_video(self, rec_id: int):
        talk_url = fill_id(self.config["DEFAULT"]["TedTalkUrl"], rec_id)
        talk_page_source = self.load_page_source(talk_url)
        if talk_page_source is None:
            raise ValueError("Error encountered while loading talk page")
        file_url = re.search(self.FILE_URL_REGEX, talk_page_source).group(0)
        return file_url

    def insert_video(self, cursor: sqlite3.Cursor, file_url: str) -> int:
        cursor.execute(
            """
            INSERT INTO videos VALUES (?)
            """,
            [file_url],
        )
        return cursor.lastrowid

    def insert_quote(self, cursor: sqlite3.Cursor, quote: dict, video_id: int) -> int:
        cursor.execute(
            """
            INSERT INTO subtitles VALUES (?, ?, ?)
            """,
            [quote["content"], quote["startTime"], quote["startTime"] + quote["duration"]],
        )
        quote_id = cursor.lastrowid
        cursor.execute(
            """
            INSERT INTO video_to_quote VALUES (?, ?)
            """,
            [video_id, quote_id],
        )
        return quote_id

    def insert_words_from_quote(self, cursor: sqlite3.Cursor, quote: dict, quote_id: int):
        for word in self.split_quote(quote["content"]):
            cursor.execute(
                """
                INSERT OR IGNORE INTO words VALUES (?)    
                """,
                [word],
            )

            cursor.execute(
                """
                SELECT ROWID FROM words where word=?
                """,
                [word],
            )
            word_id = cursor.fetchone()[0]
            cursor.execute(
                """
                INSERT OR IGNORE INTO word_to_quote VALUES (?, ?)
                """,
                [word_id, quote_id],
            )

    def load_subtitles(self, rec_id: int):
        subtitles_url = fill_id(self.config["DEFAULT"]["TedSubUrl"], rec_id)
        subtitle_page = self.load_page_source(subtitles_url)
        if subtitle_page is None:
            raise ValueError("Error encountered while loading subtitles")
        subtitles = json.loads(subtitle_page)["captions"]
        return subtitles

    def load_records(self):
        conn = self.create_conn()
        curs = conn.cursor()
        pbar = tqdm(range(1, self.n_of_records + 1))
        postfix = {"Success": 0, "Fail": 0}
        pbar.set_postfix(postfix)

        def fail():
            postfix["Fail"] += 1
            curs.execute("ROLLBACK")
            pbar.set_postfix(postfix)

        def success():
            postfix["Success"] += 1
            pbar.set_postfix(postfix)

        for rec_id in pbar:
            try:
                video_url = self.extract_video(rec_id)
                video_id = self.insert_video(curs, video_url)

                subtitles = self.load_subtitles(rec_id)

                for quote in subtitles:
                    if self.is_technical(quote):
                        continue
                    quote_id = self.insert_quote(curs, quote, video_id)
                    self.insert_words_from_quote(curs, quote, quote_id)
                conn.commit()
                success()
            except ValueError:
                fail()

        curs.close()
        conn.close()
