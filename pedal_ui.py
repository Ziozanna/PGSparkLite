#!/usr/bin/env python3
"""
Pedaliera software per Spark Go — simula i footswitch fisici.
Si collega al server PGSparkLite via SocketIO.
"""

import tkinter as tk
import socketio

SERVER_URL = "http://127.0.0.1:5000"

# SocketIO events (stessi del client web originale)
EVT_PEDAL_STATUS   = "pedal_status"
EVT_UPDATE_ONOFF   = "update_onoff"
EVT_UPDATE_PRESET  = "update_preset"
EVT_CONN_MSG       = "connection_message"
EVT_CONN_SUCCESS   = "connection_success"
EVT_CONN_LOST      = "connection-lost"

sio = socketio.Client()

PRESET_COLORS  = ["#e74c3c", "#e67e22", "#2ecc71", "#3498db"]
ON_COLOR       = "#2ecc71"
OFF_COLOR      = "#555555"
BG             = "#1a1a2e"
BTN_FG         = "#ffffff"
FONT_LABEL     = ("Helvetica", 9)
FONT_PRESET    = ("Helvetica", 18, "bold")
FONT_FX        = ("Helvetica", 11, "bold")


class PedalUI:
    def __init__(self, root):
        self.root = root
        root.title("Spark Go — Pedaliera")
        root.configure(bg=BG)
        root.resizable(False, False)

        self._preset     = 0
        self._fx_state   = {"drive": False, "mod": False,
                             "delay": False, "reverb": False}
        self._fx_btns    = {}
        self._preset_btns = []
        self._status_var = tk.StringVar(value="Non connesso")

        self._build_ui()
        self._bind_keys()
        self._connect_socketio()

    # ------------------------------------------------------------------ #
    #  UI construction
    # ------------------------------------------------------------------ #

    def _build_ui(self):
        # Status bar
        tk.Label(self.root, textvariable=self._status_var,
                 bg=BG, fg="#aaaaaa", font=FONT_LABEL,
                 anchor="w", padx=10).pack(fill="x")

        # Preset buttons
        preset_frame = tk.Frame(self.root, bg=BG, pady=8)
        preset_frame.pack()
        tk.Label(preset_frame, text="PRESET", bg=BG,
                 fg="#888888", font=FONT_LABEL).grid(
            row=0, column=0, columnspan=4)

        for i in range(4):
            btn = tk.Button(
                preset_frame,
                text=str(i + 1),
                width=5, height=2,
                bg=OFF_COLOR, fg=BTN_FG,
                activebackground=PRESET_COLORS[i],
                font=FONT_PRESET,
                relief="flat",
                command=lambda n=i: self._select_preset(n),
            )
            btn.grid(row=1, column=i, padx=6, pady=4)
            self._preset_btns.append(btn)

        # FX toggle buttons
        fx_frame = tk.Frame(self.root, bg=BG, pady=4)
        fx_frame.pack()
        tk.Label(fx_frame, text="EFFETTI", bg=BG,
                 fg="#888888", font=FONT_LABEL).grid(
            row=0, column=0, columnspan=4)

        fx_labels = [("DRIVE", "drive"), ("MOD", "mod"),
                     ("DELAY", "delay"), ("REVERB", "reverb")]

        for col, (label, key) in enumerate(fx_labels):
            btn = tk.Button(
                fx_frame,
                text=label,
                width=7, height=2,
                bg=OFF_COLOR, fg=BTN_FG,
                activebackground=ON_COLOR,
                font=FONT_FX,
                relief="flat",
                command=lambda k=key: self._toggle_fx(k),
            )
            btn.grid(row=1, column=col, padx=6, pady=4)
            self._fx_btns[key] = btn

        # Hint
        hint = "Tasti: 1-4 preset  |  D mod dela R reverb"
        tk.Label(self.root, text=hint, bg=BG, fg="#555555",
                 font=FONT_LABEL).pack(pady=(2, 6))

    def _bind_keys(self):
        for i in range(4):
            self.root.bind(str(i + 1), lambda e, n=i: self._select_preset(n))
        self.root.bind("d", lambda e: self._toggle_fx("drive"))
        self.root.bind("m", lambda e: self._toggle_fx("mod"))
        self.root.bind("e", lambda e: self._toggle_fx("delay"))
        self.root.bind("r", lambda e: self._toggle_fx("reverb"))

    # ------------------------------------------------------------------ #
    #  Actions → server
    # ------------------------------------------------------------------ #

    def _select_preset(self, index):
        try:
            sio.emit("change_preset", {"value": index})
        except Exception:
            pass
        self._update_preset_display(index)

    def _toggle_fx(self, key):
        try:
            sio.emit("toggle_effect_onoff", {"effect_type": key})
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    #  Display updates
    # ------------------------------------------------------------------ #

    def _update_preset_display(self, index):
        self._preset = index
        for i, btn in enumerate(self._preset_btns):
            btn.configure(
                bg=PRESET_COLORS[i] if i == index else OFF_COLOR)

    def _update_fx_display(self, key, is_on):
        self._fx_state[key] = is_on
        btn = self._fx_btns.get(key)
        if btn:
            btn.configure(bg=ON_COLOR if is_on else OFF_COLOR)

    def _set_status(self, msg):
        self._status_var.set(msg)

    # ------------------------------------------------------------------ #
    #  SocketIO
    # ------------------------------------------------------------------ #

    def _connect_socketio(self):
        @sio.event
        def connect():
            self.root.after(0, self._set_status, "Connesso al server")

        @sio.event
        def disconnect():
            self.root.after(0, self._set_status, "Server disconnesso")

        @sio.on(EVT_PEDAL_STATUS)
        def on_pedal_status(data):
            preset = data.get("preset", 0)
            self.root.after(0, self._update_preset_display, preset)
            for key in ("drive", "mod", "delay", "reverb"):
                state = data.get(key, "Off") == "On"
                self.root.after(0, self._update_fx_display, key, state)

        @sio.on(EVT_UPDATE_ONOFF)
        def on_update_onoff(data):
            key   = data.get("effect_type", "")
            state = data.get("state", "Off") == "On"
            if key in self._fx_btns:
                self.root.after(0, self._update_fx_display, key, state)

        @sio.on(EVT_UPDATE_PRESET)
        def on_update_preset(data):
            preset = data.get("value", 0)
            self.root.after(0, self._update_preset_display, preset)

        @sio.on(EVT_CONN_MSG)
        def on_conn_msg(data):
            msg = data.get("message", "")
            self.root.after(0, self._set_status, msg)

        @sio.on(EVT_CONN_LOST)
        def on_conn_lost(data):
            self.root.after(0, self._set_status, "Connessione amp persa")

        import threading
        def try_connect():
            try:
                sio.connect(SERVER_URL)
            except Exception as e:
                self.root.after(0, self._set_status, f"Server non raggiungibile: {e}")

        threading.Thread(target=try_connect, daemon=True).start()


def main():
    root = tk.Tk()
    app  = PedalUI(root)
    root.mainloop()
    sio.disconnect()


if __name__ == "__main__":
    main()
