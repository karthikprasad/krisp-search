from flask import Flask, request, render_template
import retriever

app = Flask(__name__)

@app.route('/')
def home_page():
    return render_template('index.html')

@app.route('/search')
def get_results():
    q = request.args.get('q', '')
    results = retriever.processQuery(q)
    return render_template("results.html", results=results, query=q)

if __name__ == '__main__':
    app.run(debug=True)