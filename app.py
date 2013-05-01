import json

from flask import Flask, render_template
from stream import Session

app = Flask(__name__)
app.config.from_object(__name__)

@app.route('/')
def index():
    result = Session.execute("""
        select datetime((strftime('%s', created_at) / 900) * 900, 'unixepoch') interval,
        count(*) count
        from tweet
        group by interval
        order by interval;
    """)
    results = result.fetchall()
    d = {}
    for a, b in results:
        d[str(a)] = b
    json_results = json.dumps(d)
    return render_template('index.html', results=json_results)

if __name__ == '__main__':
    app.run(debug=True)
