import sqlite3
from typing import Optional, List, Any

import spacy
from flask import Flask, g, request, render_template

app = Flask(__name__)
MAIN_DATABASE = 'ted_quotes.db'
TEST_DATABASE = 'ted_quotes_test.db'
nlp = spacy.load("en_core_web_sm")


def get_db() -> sqlite3.Connection:
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(MAIN_DATABASE)
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
        limit = 5

    cur = get_db().cursor()
    links = get_link(cur, query, limit, offset)
    if links is None:
        return render_template("error.html", message="Wrong query")
    if len(links) == 0:
        return render_template("error.html", message="I don't have this word in my database")
    cur.close()
    return render_template('search_result.html', links=links)


def get_link(cursor: sqlite3.Cursor, query: str, limit: int, offset: int) -> Optional[List[Any]]:
    doc = nlp(query)
    words = [w.lemma_ for w in doc]
    if len(words) != 1:
        return None
    word = words[0]
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


if __name__ == '__main__':
    app.run(debug=True)
