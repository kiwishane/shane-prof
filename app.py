from flask import Flask, render_template

app = Flask(__name__)

server = app.server

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/about")
def about():
    return render_template("about.html")

#testing GIT
#again testing

app.run(debug=True, host="127.0.0.1", port=3000)


