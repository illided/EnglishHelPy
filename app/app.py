import os.path
import re
import sqlite3
from typing import Optional, List, Tuple

import requests
import spacy
from flask import Flask, g, request, render_template

app = Flask(__name__)
MAIN_DATABASE = 'ted_quotes.db'
TEST_DATABASE = 'ted_quotes_test.db'
nlp = spacy.load("en_core_web_sm")


def get_db() -> sqlite3.Connection:
    db = getattr(g, '_database', None)
    db_path = os.path.join(os.getcwd(), MAIN_DATABASE)
    if not os.path.exists(db_path):
        print("Main database not found. Trying to connect to test")
        db_path = os.path.join(os.getcwd(), TEST_DATABASE)
    if db is None:
        db = g._database = sqlite3.connect(db_path)
    return db


@app.route('/')
def index():
    query = request.args.get('query')
    if query is None:
        return render_template('welcome.html')

    offset = request.args.get('offset')
    if offset is None:
        offset = 0
    limit = request.args.get('limit')
    if limit is None:
        limit = 6

    word = process_query(query)
    if word is None:
        return render_template("error.html", message="Wrong query")
    definition = get_definition(word)
    cur = get_db().cursor()
    links = get_link(cur, word, limit, offset)
    cur.close()
    if len(links) == 0 and definition is None:
        return render_template("error.html", message="Couldn't find definition or usages")
    return render_template('search_result.html', links=links, word_analysis=definition)


def process_query(query: str) -> Optional[str]:
    doc = nlp(query)
    words = [w.lemma_ for w in doc]
    if len(words) != 1:
        return None
    return words[0]


def get_link(cursor: sqlite3.Cursor, word: str, limit: int, offset: int) -> List[Tuple[str, str]]:
    cursor.execute(
        """
        select link, quote, start_time, end_time from words
        join word_to_quote on words.ROWID = word_id
        join subtitles on word_to_quote.quote_id=subtitles.ROWID
        join video_to_quote on video_to_quote.quote_id=word_to_quote.quote_id
        join videos on videos.ROWID = video_id
        WHERE word = ? group by link limit ? offset ?;
        """
        , [word, limit, offset]
    )
    results = cursor.fetchall()
    links = []
    quotes = []
    for r in results:
        link, quote, start_time, end_time = r
        start_shifted = max(0, int(start_time) // 1000 - 3)
        end_shifted = int(end_time) // 1000 + 5
        links.append(link + f'#t={start_shifted},{end_shifted}')
        quotes.append(quote)
    return list(zip(links, quotes))


def get_definition(word: str) -> Optional[dict]:
    if not re.match(r'[A-Za-z]+', word):
        return None
    response = requests.get(f'https://api.dictionaryapi.dev/api/v2/entries/en/{word}')
    if not response.ok:
        return None
    as_json = response.json()
    if isinstance(as_json, dict) and as_json['title'] == "No Definitions Found":
        return None
    return as_json[0]


if __name__ == '__main__':
    app.run(debug=False)
