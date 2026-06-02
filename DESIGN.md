# Internet GoMoKu (Connect-5) — Design Document

**Group members / student numbers:** _______________________________________

**Game URL (client):** `https://<you>.github.io/<repo>/`
**Server URL:** `wss://<your-service>.onrender.com`

---

## 1. System structure

The system follows the **client–server model**.

- **Client** (`index.html`): a pure graphical interface that runs in the web
  browser. It renders the 10×10 board, players' names, and a result field. It
  has *no* game logic of its own — it waits for the user to click a square,
  sends that move to the server, and draws whatever state the server reports.
- **Server** (`server.py`): a long-running Python process. It owns the
  authoritative board for every match, decides whose turn it is, validates and
  records each move, relays moves to the opponent, and judges the end of the
  game.

The original assignment specified a Java applet client and a Java
stream-socket server. We keep that client–server structure but realise the
server on the **Berkeley stream-socket API taught in the course lecture**
(T6, "Socket Communication"): the server creates a socket, binds it, listens,
accepts connections, exchanges data with recv/send, and closes — the exact
`socket → bind → listen → accept → read/write → close` lifecycle from the
slides, annotated step-by-step in `server.py`.

No current browser executes Java applets, and — more fundamentally — no
browser can open a raw TCP socket at all (a deliberate browser security
restriction). The browser's only persistent stream is the **WebSocket**,
which is itself a `SOCK_STREAM` (TCP) connection with a standard handshake on
top. We therefore implement the WebSocket handshake and frame codec **by
hand, directly on the raw socket**, so the lecture's socket calls remain the
visible foundation of the server rather than being hidden inside a library.
Everything the client and server exchange travels over that single
connection-oriented, in-order TCP stream (the "stream service" of slide 5).

Because the client is only static files, it is hosted on **GitHub Pages**. The
server is a live process and is hosted on **Render** (free tier). This split is
required: a static host cannot keep a process alive or hold socket connections.

## 2. Client–server communication

Communication is a single WebSocket per client, carrying small **JSON** text
frames.

**Client → Server**
- `{"type":"join","name":<str>}` — request a match.
- `{"type":"move","row":<int>,"col":<int>}` — attempt a move.
- `{"type":"reset"}` — start a new game with the same opponent.

**Server → Client**
- `{"type":"waiting"}` — connected, awaiting an opponent.
- `{"type":"start","color":...,"you":...,"opponent":...,"turn":"black"}` —
  match formed; tells each player their stone colour and who moves first.
- `{"type":"move","row":r,"col":c,"color":...,"turn":...}` — a recorded move,
  broadcast to **both** players so the stone appears on both displays
  immediately.
- `{"type":"over","result":"win"|"draw","winner":...,"line":[...]}` — the
  server's verdict, with the five winning cells.
- `{"type":"error","message":...}` — an attempted move was rejected.
- `{"type":"opponent_left"}` — the opponent disconnected.

A typical exchange: a player clicks a cell → client sends `move` → server
validates it against the authoritative board, records it, then **broadcasts**
the resulting `move` (and possibly `over`) to both clients, which redraw.

## 3. Supporting multiple players concurrently

The server keeps a set of independent **`Game`** objects, one per pair of
players. Matchmaking holds at most one "open" room: the first player to join
creates a room and receives `waiting`; the next joiner fills it, the room
closes, and both players receive `start`. The next join opens a fresh room.

Each connection accepted by `accept()` is handled in its **own thread**, so
many connections — and therefore many game pairs — are served at the same
time, each isolated with its own board, turn pointer, and pair of sockets.
This mirrors the lecture's **Protocol Control Block** discussion (slides
21–28): every live connection is a distinct session identified by its own
socket, and the server tracks them independently. A shared lock guards the
matchmaking step so two simultaneous joins cannot corrupt the open-room
pointer, and each game has its own lock so moves are applied atomically.

## 4. Tolerating invalid moves

All validation is **server-side**; the client is never trusted. The server
rejects, with an `error` message and no state change, any move that is:

- **out of turn** — the sender's colour is not the colour to move;
- **on an occupied cell** — the target already holds a stone;
- **off the board** — row/column outside 0–9;
- **after the game is over**, or **before an opponent has joined**.

This directly covers the assignment's example of a player moving twice before
the opponent responds: the second move arrives while it is still the opponent's
turn, so the server discards it and tells the offending client why.

## 5. Win/draw detection

When a move is recorded, the server checks the four axes through that cell
(horizontal, vertical, and both diagonals), counting same-colour stones in each
direction. Five or more in a line ends the game; the server marks the winner and
returns the exact five winning cells, which the client highlights. If the board
fills with no line, the server declares a draw.

## 6. Other design decisions

- **Black moves first**, by convention; the first player to connect in each
  pair is assigned Black.
- **Reconnection / disconnect:** if a player leaves, the survivor is notified
  with `opponent_left` and the room is cleaned up.
- **Reset** lets the same pair replay without reconnecting.
- **Free-tier note:** Render free services sleep when idle, so the very first
  connection after a quiet period may take a few seconds to wake — a normal
  trade-off for zero-cost hosting, with no effect on gameplay once awake.
