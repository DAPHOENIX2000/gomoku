"""
Internet GoMoKu (Connect-5) — Stream-Socket Game Server
=======================================================

Distributed Systems Project.

This server is built directly on the Berkeley stream-socket API taught in
the lecture (T6, "Socket Communication"). The exact lifecycle from the
slides is used, with no networking library hiding it:

    s = socket(AF_INET, SOCK_STREAM, 0)     # slide 10  — create
    bind(s, addr)                           # slide 11  — bind
    listen(s, backlog)                      # slide 15  — listen
    new_s = accept(s)                       # slide 16  — accept
    recv(new_s) / send(new_s)               # slide 18  — exchange
    close(new_s)                            # slide 18  — close

Browsers cannot open a raw TCP socket, so the ONE thing layered on top of
the raw stream is the WebSocket handshake + frame codec (RFC 6455),
implemented by hand here so the socket calls stay visible. Everything the
client and server exchange still travels over this single SOCK_STREAM
(virtual-circuit / TCP) connection — the connection-oriented, in-order
"stream service" described on slide 5.

Concurrency (the assignment's "several pairs of players concurrently"):
each accepted connection is handled in its own thread, and players are
matched into independent Game rooms. This mirrors the lecture's Protocol
Control Block discussion (slides 21-28): every connection is a distinct
session keyed by its own socket.

Game protocol (JSON text frames over the stream):

  Client -> Server
    {"type":"join","name":<str>}
    {"type":"move","row":<int>,"col":<int>}
    {"type":"reset"}

  Server -> Client
    {"type":"waiting"}
    {"type":"start","color":...,"you":...,"opponent":...,"turn":"black"}
    {"type":"move","row":r,"col":c,"color":...,"turn":...}
    {"type":"over","result":"win"|"draw","winner":...,"line":[...]}
    {"type":"opponent_left"}
    {"type":"error","message":...}
"""

import socket
import threading
import hashlib
import base64
import struct
import json
import os
import itertools

BOARD_SIZE = 10          # assignment specifies a 10x10 grid
CONNECT = 5              # five in a row wins
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"   # RFC 6455 magic string

# ===========================================================================
#  PART 1 — WebSocket framing over a raw stream socket
#  (the only layer on top of the lecture's socket calls)
# ===========================================================================


class StreamPeer:
    """Wraps one accepted SOCK_STREAM connection and speaks WebSocket over it.

    All I/O goes through the raw socket's recv()/send() — the same
    read/write-on-a-socket model from slide 18. The WebSocket handshake and
    frame (de)coding are done explicitly so nothing hides the stream.
    """

    def __init__(self, conn):
        self.conn = conn                 # the new_s returned by accept()
        self._buf = b""
        self.open = False

    # --- the opening handshake (HTTP upgrade carried over the stream) ----
    def handshake(self):
        request = self._read_until(b"\r\n\r\n")
        if request is None:
            return False
        headers = {}
        for line in request.split(b"\r\n")[1:]:
            if b":" in line:
                k, v = line.split(b":", 1)
                headers[k.strip().lower()] = v.strip()
        key = headers.get(b"sec-websocket-key")
        if not key:
            return False
        accept = base64.b64encode(
            hashlib.sha1(key + WS_GUID.encode()).digest()
        ).decode()
        response = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
        )
        self.conn.sendall(response.encode())     # send() on the stream
        self.open = True
        return True

    def _read_until(self, delim):
        while delim not in self._buf:
            chunk = self.conn.recv(4096)         # recv() on the stream
            if not chunk:
                return None
            self._buf += chunk
        idx = self._buf.index(delim) + len(delim)
        head, self._buf = self._buf[:idx], self._buf[idx:]
        return head

    def _read_exact(self, n):
        while len(self._buf) < n:
            chunk = self.conn.recv(4096)
            if not chunk:
                return None
            self._buf += chunk
        data, self._buf = self._buf[:n], self._buf[n:]
        return data

    # --- decode one client->server frame --------------------------------
    def recv_message(self):
        first = self._read_exact(2)
        if first is None:
            return None
        b0, b1 = first[0], first[1]
        opcode = b0 & 0x0F
        masked = b1 & 0x80
        length = b1 & 0x7F
        if length == 126:
            ext = self._read_exact(2)
            if ext is None:
                return None
            length = struct.unpack(">H", ext)[0]
        elif length == 127:
            ext = self._read_exact(8)
            if ext is None:
                return None
            length = struct.unpack(">Q", ext)[0]
        mask = self._read_exact(4) if masked else b"\x00\x00\x00\x00"
        if mask is None:
            return None
        payload = self._read_exact(length) if length else b""
        if payload is None:
            return None
        if masked:
            payload = bytes(p ^ mask[i % 4] for i, p in enumerate(payload))

        if opcode == 0x8:        # close
            return None
        if opcode == 0x9:        # ping -> pong
            self._send_frame(payload, opcode=0xA)
            return self.recv_message()
        if opcode == 0xA:        # pong
            return self.recv_message()
        return payload.decode("utf-8", "replace")

    # --- encode one server->client frame (unmasked, per RFC) -------------
    def _send_frame(self, data, opcode=0x1):
        if isinstance(data, str):
            data = data.encode("utf-8")
        header = bytearray([0x80 | opcode])
        n = len(data)
        if n < 126:
            header.append(n)
        elif n < (1 << 16):
            header.append(126)
            header += struct.pack(">H", n)
        else:
            header.append(127)
            header += struct.pack(">Q", n)
        try:
            self.conn.sendall(bytes(header) + data)   # send() on the stream
        except OSError:
            self.open = False

    def send_message(self, text):
        self._send_frame(text, opcode=0x1)

    def close(self):
        try:
            self._send_frame(b"", opcode=0x8)
            self.conn.close()                         # close() the stream
        except OSError:
            pass
        self.open = False


# ===========================================================================
#  PART 2 — Game logic (board, rules, server-side judging)
# ===========================================================================


class Game:
    _ids = itertools.count(1)

    def __init__(self):
        self.id = next(Game._ids)
        self.board = [[None] * BOARD_SIZE for _ in range(BOARD_SIZE)]
        self.players = {}          # color -> StreamPeer
        self.names = {}            # color -> name
        self.turn = "black"        # black moves first
        self.over = False
        self.lock = threading.Lock()

    def is_full(self):
        return len(self.players) == 2

    def color_of(self, peer):
        for color, p in self.players.items():
            if p is peer:
                return color
        return None

    def add_player(self, peer, name):
        color = "black" if "black" not in self.players else "white"
        self.players[color] = peer
        self.names[color] = name or color
        return color

    def remove_player(self, peer):
        color = self.color_of(peer)
        if color is not None:
            del self.players[color]
        return color

    def apply_move(self, peer, row, col):
        if self.over:
            return False, "The game is already over."
        if not self.is_full():
            return False, "Waiting for an opponent."
        color = self.color_of(peer)
        if color != self.turn:
            return False, "It is not your turn."
        if not (0 <= row < BOARD_SIZE and 0 <= col < BOARD_SIZE):
            return False, "Move is off the board."
        if self.board[row][col] is not None:
            return False, "That cell is already taken."
        self.board[row][col] = color
        return True, None

    def winning_line(self, row, col):
        color = self.board[row][col]
        for dr, dc in [(0, 1), (1, 0), (1, 1), (1, -1)]:
            cells = [(row, col)]
            r, c = row + dr, col + dc
            while 0 <= r < BOARD_SIZE and 0 <= c < BOARD_SIZE and self.board[r][c] == color:
                cells.append((r, c)); r += dr; c += dc
            r, c = row - dr, col - dc
            while 0 <= r < BOARD_SIZE and 0 <= c < BOARD_SIZE and self.board[r][c] == color:
                cells.insert(0, (r, c)); r -= dr; c -= dc
            if len(cells) >= CONNECT:
                return cells[:CONNECT]
        return None

    def board_full(self):
        return all(self.board[r][c] is not None
                   for r in range(BOARD_SIZE) for c in range(BOARD_SIZE))

    def reset(self):
        self.board = [[None] * BOARD_SIZE for _ in range(BOARD_SIZE)]
        self.turn = "black"
        self.over = False


# ===========================================================================
#  PART 3 — Matchmaking + per-connection handling (concurrent pairs)
# ===========================================================================

waiting_game = None
match_lock = threading.Lock()


def send(peer, payload):
    if peer and peer.open:
        peer.send_message(json.dumps(payload))


def broadcast(game, payload):
    for p in list(game.players.values()):
        send(p, payload)


def do_join(peer, name, state):
    global waiting_game
    with match_lock:
        if waiting_game is None:
            game = Game()
            waiting_game = game
            game.add_player(peer, name)
            state["game"] = game
            send(peer, {"type": "waiting"})
        else:
            game = waiting_game
            game.add_player(peer, name)
            state["game"] = game
            waiting_game = None
            for color in ("black", "white"):
                send(game.players[color], {
                    "type": "start", "color": color,
                    "you": game.names[color],
                    "opponent": game.names["white" if color == "black" else "black"],
                    "turn": game.turn,
                })


def do_move(peer, game, row, col):
    with game.lock:
        ok, err = game.apply_move(peer, row, col)
        if not ok:
            send(peer, {"type": "error", "message": err})
            return
        color = game.board[row][col]
        line = game.winning_line(row, col)
        if line:
            game.over = True
            broadcast(game, {"type": "move", "row": row, "col": col,
                             "color": color, "turn": None})
            broadcast(game, {"type": "over", "result": "win",
                             "winner": color, "line": line})
        elif game.board_full():
            game.over = True
            broadcast(game, {"type": "move", "row": row, "col": col,
                             "color": color, "turn": None})
            broadcast(game, {"type": "over", "result": "draw",
                             "winner": None, "line": []})
        else:
            game.turn = "white" if color == "black" else "black"
            broadcast(game, {"type": "move", "row": row, "col": col,
                             "color": color, "turn": game.turn})


def do_reset(game):
    if game and game.is_full():
        with game.lock:
            game.reset()
        for color in ("black", "white"):
            send(game.players[color], {
                "type": "start", "color": color,
                "you": game.names[color],
                "opponent": game.names["white" if color == "black" else "black"],
                "turn": game.turn, "reset": True,
            })


def serve_connection(conn, addr):
    """Runs in its own thread for each accept()ed stream connection."""
    global waiting_game
    peer = StreamPeer(conn)
    state = {"game": None}
    try:
        if not peer.handshake():
            conn.close()
            return
        while True:
            raw = peer.recv_message()
            if raw is None:
                break
            try:
                msg = json.loads(raw)
            except (ValueError, TypeError):
                send(peer, {"type": "error", "message": "Bad message."})
                continue
            mtype = msg.get("type")
            game = state["game"]
            if mtype == "join":
                do_join(peer, msg.get("name", "Player"), state)
            elif mtype == "move" and game is not None:
                do_move(peer, game, int(msg.get("row", -1)),
                        int(msg.get("col", -1)))
            elif mtype == "reset" and game is not None:
                do_reset(game)
            else:
                send(peer, {"type": "error", "message": "Unknown action."})
    except OSError:
        pass
    finally:
        game = state["game"]
        if game is not None:
            with match_lock:
                game.remove_player(peer)
                if waiting_game is game and not game.players:
                    waiting_game = None
            for p in list(game.players.values()):
                send(p, {"type": "opponent_left"})
        peer.close()


# ===========================================================================
#  PART 4 — The lecture's stream-socket server lifecycle (slides 8-20)
# ===========================================================================


def main():
    port = int(os.environ.get("PORT", "8765"))

    # 1. Create a socket                                         (slide 10)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # 2. Bind socket to an address                               (slide 11)
    s.bind(("0.0.0.0", port))

    # 3. Set the socket for listening                            (slide 15)
    s.listen(5)
    print(f"GoMoKu stream-socket server listening on 0.0.0.0:{port}")

    try:
        while True:
            # 4. Accept a connection                             (slide 16)
            conn, addr = s.accept()
            # Hand the new session socket to its own thread so several
            # pairs of players are served concurrently (slides 21-28).
            t = threading.Thread(target=serve_connection,
                                 args=(conn, addr), daemon=True)
            t.start()
    except KeyboardInterrupt:
        pass
    finally:
        # 5. Close the socket                                    (slide 18)
        s.close()


if __name__ == "__main__":
    main()
