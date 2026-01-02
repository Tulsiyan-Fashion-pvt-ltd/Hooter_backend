from flask import Flask
from flask_cors import CORS
from pages import page # importing the page blueprint for the page routes


app = Flask(__name__)
CORS(app, supports_credentials=True,
    origins=["http://localhost:5173"])

app.register_blueprint(page)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8000)