import sqlite3
from typing import Optional, List, Any

import spacy
from flask import Flask, g, request

app = Flask(__name__)
MAIN_DATABASE = 'ted_quotes.db'
TEST_DATABASE = 'ted_quotes_test.db'
nlp = spacy.load("en_core_web_trf")


def get_db() -> sqlite3.Connection:
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(MAIN_DATABASE)
    return db


@app.route('/')
def index():
    cur = get_db().cursor()
    query = request.args.get('query')
    if query is None:
        return "Please provide query"
    offset = request.args.get('offset')
    if offset is None:
        offset = 0
    limit = request.args.get('limit')
    if limit is None:
        limit = 5
    links = get_link(cur, query, limit, offset)
    clickable = [f'<a href="{link}">{link}</a>' for link in links]
    return '<br>'.join(clickable)



def get_link(cursor: sqlite3.Cursor, query: str, limit: int, offset: int) -> Optional[List[Any]]:
    doc = nlp(query)
    words = [w.lemma_ for w in doc]
    if len(words) != 1:
        return None
    word = words[0]
    cursor.execute(
        """
        select link, start_time, end_time from words
        join word_to_quote on words.ROWID = word_id
        join subtitles on word_to_quote.quote_id=subtitles.ROWID
        join video_to_quote on video_to_quote.quote_id=word_to_quote.quote_id
        join videos on videos.ROWID = video_id
        WHERE word = ? limit ? offset ?;
        """
    , [word, limit, offset])
    results = cursor.fetchall()
    links = []
    for r in results:
        link, start_time, end_time = r
        start_shifted = max(0, int(start_time) // 1000 - 5)
        end_shifted = int(end_time) // 1000 + 10
        links.append(link + f'#t={start_shifted},{end_shifted}')
    return links


if __name__ == '__main__':
    app.run(debug=True)
