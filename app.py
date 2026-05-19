# app.py — Task 3.1 (Pass): minimal Flask app to prove Render deployment works
from flask import Flask

app = Flask(__name__)

@app.route("/")
def home():
    return """
    <html>
    <head><title>FloodWatch — Render Deploy</title></head>
    <body style="font-family:sans-serif; max-width:600px; margin:80px auto; text-align:center;">
        <h1>🌊 FloodWatch</h1>
        <p>Deployed successfully to Render.</p>
        <p style="color:green; font-weight:bold;">✓ Task 3.1 — Pass</p>
        <hr>
        <small>SWE40006 Portfolio 3 — Student ID: 102782478</small>
    </body>
    </html>
    """

if __name__ == "__main__":
    app.run()
