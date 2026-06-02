# Internet GoMoKu (Connect-5)

A web-based, real-time, two-player GoMoKu game built for the Distributed
Systems project. Two people in **separate browsers** play live: every move
appears instantly on both screens, the **server** records moves and judges
the winner, and **multiple pairs of players** can play at the same time.

## What this is (and how it maps to the assignment)

The original brief specified a **Java applet** client and a **Java socket
server** communicating over raw **stream (TCP) sockets**. The server here is
built directly on the **Berkeley stream-socket API taught in the course
lecture** (`socket()` → `bind()` → `listen()` → `accept()` → recv/send →
`close()`) — see `server.py` Part 4, with each step annotated to its slide.

Browsers can't open a raw TCP socket (a built-in browser security rule), so
the **only** thing layered on top of the raw stream is the WebSocket
handshake and frame codec, hand-written in `server.py` Part 1 so the socket
calls stay fully visible rather than hidden inside a library. All game data
still travels over that one `SOCK_STREAM` (TCP, connection-oriented,
in-order) connection.

| Assignment requirement            | How it's met here                                       |
|-----------------------------------|---------------------------------------------------------|
| Client = graphical interface only | `index.html` — draws the board, sends moves, renders state |
| Server in a server-side language  | `server.py` — Python, runs on the host                  |
| Stream-socket communication       | Raw `SOCK_STREAM` socket (lecture API), WebSocket framing on top |
| Server records & relays moves     | Server holds the board; clients never trust each other  |
| Server judges the result          | Win/draw detection is **server-side only**              |
| Several pairs concurrently        | One thread per accepted socket; each pair its own `Game` room |
| Tolerate invalid moves            | Out-of-turn, occupied-cell, off-board, post-game moves rejected |
| 10×10 board, 5-in-a-row           | `BOARD_SIZE = 10`, `CONNECT = 5`                         |

## Architecture

```
  Browser (Black)              Browser (White)
        │  index.html                │  index.html
        │                            │
        └──── WebSocket stream ──────┴──── WebSocket stream ────┐
                                                                 ▼
                                                        server.py (Python)
                                                        ├─ Game room per pair
                                                        ├─ records every move
                                                        ├─ relays to opponent
                                                        └─ judges win / draw
```

The **client** is hosted as a static file on **GitHub Pages**.
The **server** must run as a live process, so it is hosted **free on
Render**. (GitHub Pages cannot run server code — it only serves files.)

---

## Part 1 — Deploy the server on Render (free)

1. Push this folder to a GitHub repository (see Part 2 if you haven't yet).
2. Go to <https://render.com> and sign in with GitHub.
3. Click **New → Web Service** and pick your repo.
4. Render auto-detects `render.yaml`. If asked, set:
   - **Runtime:** Python 3
   - **Build command:** `pip install -r requirements.txt`
   - **Start command:** `python server.py`
   - **Plan:** Free
5. Click **Create Web Service**. Wait for the log to show
   `GoMoKu server listening on 0.0.0.0:...`.
6. Copy your service URL, e.g. `https://gomoku-server-xxxx.onrender.com`.
   Your WebSocket address is the same URL with `wss://`:
   ```
   wss://gomoku-server-xxxx.onrender.com
   ```

> Free Render services sleep after inactivity; the first connection may
> take ~30 seconds to wake. Just click "Find a match" again if the first
> attempt times out.

---

## Part 2 — Host the client on GitHub Pages

1. Create a new repo on GitHub and push these files:
   ```bash
   git init
   git add .
   git commit -m "Internet GoMoKu"
   git branch -M main
   git remote add origin https://github.com/<you>/<repo>.git
   git push -u origin main
   ```
2. On GitHub: **Settings → Pages → Build and deployment**.
   Set **Source = Deploy from a branch**, **Branch = `main` / `(root)`**, Save.
3. After a minute your client is live at:
   ```
   https://<you>.github.io/<repo>/
   ```

---

## Part 3 — Play

1. Open the GitHub Pages URL in two browser windows (or send it to a friend).
2. In the **Server address** box, paste your `wss://...onrender.com` address.
3. Enter a name and click **Find a match** in both windows.
4. The first to connect is **Black** and moves first. Get five in a row —
   horizontal, vertical, or diagonal — to win.

---

## Run locally (for development)

```bash
python server.py            # serves on ws://localhost:8765 over a raw TCP socket
```

(No `pip install` needed — the server uses only the Python standard library.)

Then open `index.html` in two browser tabs and use `ws://localhost:8765`
as the server address.

## Files

| File              | Purpose                                            |
|-------------------|----------------------------------------------------|
| `server.py`       | Stream-socket game server (raw `SOCK_STREAM` + WS framing, rules, judging) |
| `index.html`      | Browser client — the graphical interface           |
| `requirements.txt`| Empty — server uses only the Python standard library |
| `render.yaml`     | Render deployment config                           |
| `Procfile`        | Alternate start command (Railway/Heroku-style)     |
| `DESIGN.md`       | 2-page design document required by the assignment  |
