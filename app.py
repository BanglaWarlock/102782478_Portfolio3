# app.py — Task 3.2 (Credit)
# Simple Flask web app with environment variable configuration.
# Deployed to Render with auto-deploy on git push.

import os
from datetime import datetime
from flask import Flask, render_template_string, request, redirect, url_for

app = Flask(__name__)

# These values come from Render's environment variable settings (not hardcoded)
APP_NAME    = os.environ.get("APP_NAME", "FloodWatch Monitor")
AUTHOR_NAME = os.environ.get("AUTHOR_NAME", "Student")
STUDENT_ID  = os.environ.get("STUDENT_ID", "102782478")
STAGE       = os.environ.get("STAGE", "production")

# Simple in-memory store — resets on restart (no DB yet; that's Task 3.3)
_notes: list[dict] = []

TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{{ app_name }}</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: system-ui, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; }
    header { background: #1e3a8a; padding: 16px 32px; display: flex; align-items: center; justify-content: space-between; }
    header h1 { font-size: 1.25rem; }
    header .meta { font-size: 0.75rem; color: #93c5fd; }
    .badge { display:inline-block; padding: 2px 8px; border-radius: 9999px; font-size: 0.7rem;
             background: #166534; color: #bbf7d0; margin-left: 8px; }
    .container { max-width: 720px; margin: 40px auto; padding: 0 16px; }
    .card { background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 24px; margin-bottom: 24px; }
    .card h2 { font-size: 1rem; color: #94a3b8; margin-bottom: 16px; text-transform: uppercase;
               letter-spacing: 0.05em; font-weight: 600; }
    .info-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .info-item { background: #0f172a; border-radius: 8px; padding: 12px; }
    .info-item .label { font-size: 0.7rem; color: #64748b; text-transform: uppercase; margin-bottom: 4px; }
    .info-item .value { font-weight: 600; color: #f1f5f9; }
    form { display: flex; gap: 8px; }
    input[type=text] { flex: 1; background: #0f172a; border: 1px solid #475569; border-radius: 8px;
                       color: #f1f5f9; padding: 8px 12px; font-size: 0.9rem; }
    input[type=text]:focus { outline: none; border-color: #3b82f6; }
    button[type=submit] { background: #2563eb; color: white; border: none; border-radius: 8px;
                          padding: 8px 20px; cursor: pointer; font-weight: 600; }
    button[type=submit]:hover { background: #1d4ed8; }
    .note-list { list-style: none; }
    .note-list li { padding: 10px 12px; background: #0f172a; border-radius: 8px; margin-top: 8px;
                    display: flex; justify-content: space-between; align-items: center; font-size: 0.9rem; }
    .note-list li .time { font-size: 0.7rem; color: #64748b; }
    .empty { color: #64748b; font-size: 0.9rem; font-style: italic; }
    .env-tag { font-family: monospace; background: #0f172a; border-radius: 4px; padding: 1px 6px;
               color: #38bdf8; font-size: 0.8rem; }
  </style>
</head>
<body>
<header>
  <h1>🌊 {{ app_name }} <span class="badge">{{ stage }}</span></h1>
  <div class="meta">{{ author }} · {{ student_id }} · SWE40006 Portfolio 3</div>
</header>

<div class="container">

  <!-- Environment info card -->
  <div class="card">
    <h2>Environment Variables (Task 3.2)</h2>
    <p style="font-size:0.8rem; color:#64748b; margin-bottom:16px;">
      These values are configured in Render's Environment tab — not hardcoded in the source.
    </p>
    <div class="info-grid">
      <div class="info-item">
        <div class="label">APP_NAME</div>
        <div class="value">{{ app_name }}</div>
      </div>
      <div class="info-item">
        <div class="label">AUTHOR_NAME</div>
        <div class="value">{{ author }}</div>
      </div>
      <div class="info-item">
        <div class="label">STUDENT_ID</div>
        <div class="value">{{ student_id }}</div>
      </div>
      <div class="info-item">
        <div class="label">STAGE</div>
        <div class="value">{{ stage }}</div>
      </div>
    </div>
    <p style="margin-top:16px; font-size:0.8rem; color:#64748b;">
      Deployed via: <span class="env-tag">git push → GitHub → Render auto-deploy</span>
    </p>
    <p style="margin-top:6px; font-size:0.8rem; color:#64748b;">
      Deployed at: <span class="env-tag">{{ deploy_time }}</span>
    </p>
  </div>

  <!-- Notes card — demonstrates a working web app (form submission) -->
  <div class="card">
    <h2>Quick Notes <span style="font-size:0.7rem; color:#64748b; text-transform:none; font-weight:400;">(in-memory — resets on restart; DB added in Task 3.3)</span></h2>
    <form method="POST" action="/add-note">
      <input type="text" name="note" placeholder="Type a note and press Add…" required maxlength="200">
      <button type="submit">Add</button>
    </form>
    {% if notes %}
    <ul class="note-list" style="margin-top:12px;">
      {% for n in notes %}
      <li>
        <span>{{ n.text }}</span>
        <span class="time">{{ n.time }}</span>
      </li>
      {% endfor %}
    </ul>
    {% else %}
    <p class="empty" style="margin-top:12px;">No notes yet — add one above.</p>
    {% endif %}
  </div>

</div>
</body>
</html>
"""


@app.route("/")
def home():
    return render_template_string(
        TEMPLATE,
        app_name    = APP_NAME,
        author      = AUTHOR_NAME,
        student_id  = STUDENT_ID,
        stage       = STAGE,
        notes       = _notes,
        deploy_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
    )


@app.route("/add-note", methods=["POST"])
def add_note():
    text = request.form.get("note", "").strip()
    if text:
        _notes.insert(0, {
            "text": text,
            "time": datetime.utcnow().strftime("%H:%M:%S UTC"),
        })
        if len(_notes) > 10:   # keep at most 10
            _notes.pop()
    return redirect(url_for("home"))


@app.route("/health")
def health():
    return {"status": "ok", "app": APP_NAME, "stage": STAGE}


if __name__ == "__main__":
    app.run(debug=False)
