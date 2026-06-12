#!/usr/bin/env python3
"""
Pedaliera software per Spark Go — simula i footswitch fisici.
Si collega al server PGSparkLite via SocketIO.
"""

import tkinter as tk
import socketio

SERVER_URL = "http://127.0.0.1:5000"

EVT_PEDAL_STATUS  = "pedal_status"
EVT_UPDATE_ONOFF  = "update_onoff"
EVT_UPDATE_PRESET = "update_preset"
EVT_CONN_MSG      = "connection_message"
EVT_CONN_LOST     = "connection-lost"

sio = socketio.Client()

PRESET_COLORS = ["#e74c3c", "#e67e22", "#2ecc71", "#3498db"]
ON_COLOR      = "#2ecc71"
OFF_COLOR     = "#444455"
NAV_COLOR     = "#2c3e6e"
NAV_ACTIVE    = "#4a60aa"
BG            = "#1a1a2e"
DISPLAY_BG    = "#0a0a18"
LCD_GREEN     = "#00ff88"
LCD_DIM       = "#004422"
LCD_LABEL     = "#007744"

FONT_STATUS   = ("Helvetica", 9)
FONT_PRESET   = ("Helvetica", 20, "bold")
FONT_FX       = ("Helvetica", 11, "bold")
FONT_NAV      = ("Helvetica", 16, "bold")
FONT_LCD_NUM  = ("Courier", 48, "bold")
FONT_LCD_NAME = ("Courier", 13)
FONT_LCD_INFO = ("Courier", 10)


class PedalUI:
    def __init__(self, root):
        self.root = root
        root.title("Spark Go — Pedaliera")
        root.configure(bg=BG)
        root.resizable(False, False)

        self._preset      = 0
        self._fx_state    = {"drive": False, "mod": False,
                              "delay": False, "reverb": False}
        self._fx_btns     = {}
        self._preset_btns = []
        self._status_var  = tk.StringVar(value="Non connesso")

        self._build_ui()
        self._bind_keys()
        self._connect_socketio()

    # ------------------------------------------------------------------ #
    #  UI construction
    # ------------------------------------------------------------------ #

    def _build_ui(self):
        # Status bar
        tk.Label(self.root, textvariable=self._status_var,
                 bg=BG, fg="#777788", font=FONT_STATUS,
                 anchor="w", padx=10).pack(fill="x")

        # ── DISPLAY LCD ──────────────────────────────────────────────── #
        display_outer = tk.Frame(self.root, bg="#0d0d20", bd=3, relief="groove")
        display_outer.pack(fill="x", padx=12, pady=(4, 0))

        display = tk.Frame(display_outer, bg=DISPLAY_BG, padx=14, pady=10)
        display.pack(fill="x")

        # Riga 1: etichetta "PRESET" a sx, numero grande a dx
        top_row = tk.Frame(display, bg=DISPLAY_BG)
        top_row.pack(fill="x")

        tk.Label(top_row, text="PRESET", bg=DISPLAY_BG, fg=LCD_LABEL,
                 font=FONT_LCD_INFO, anchor="sw").pack(side="left", pady=(12, 0))

        self._disp_num = tk.Label(top_row, text="—", bg=DISPLAY_BG,
                                   fg=LCD_GREEN, font=FONT_LCD_NUM, anchor="e")
        self._disp_num.pack(side="right")

        # Riga 2: nome preset
        self._disp_name = tk.Label(display, text="",
                                    bg=DISPLAY_BG, fg=LCD_GREEN,
                                    font=FONT_LCD_NAME, anchor="w")
        self._disp_name.pack(fill="x", pady=(0, 6))

        # Riga 3: BPM + indicatori FX
        info_row = tk.Frame(display, bg=DISPLAY_BG)
        info_row.pack(fill="x")

        self._disp_bpm = tk.Label(info_row, text="— BPM",
                                   bg=DISPLAY_BG, fg=LCD_LABEL,
                                   font=FONT_LCD_INFO, width=8, anchor="w")
        self._disp_bpm.pack(side="left")

        self._disp_fx = {}
        for key, label in [("drive", "DRV"), ("mod", "MOD"),
                            ("delay", "DLY"), ("reverb", "REV")]:
            lbl = tk.Label(info_row, text=f"[{label}]",
                           bg=DISPLAY_BG, fg=LCD_DIM, font=FONT_LCD_INFO)
            lbl.pack(side="left", padx=5)
            self._disp_fx[key] = lbl

        # ── NAVIGAZIONE PRESET ────────────────────────────────────────── #
        nav_frame = tk.Frame(self.root, bg=BG, pady=10)
        nav_frame.pack()

        # ◄ PREV
        tk.Button(nav_frame, text="◄", width=4, height=2,
                  bg=NAV_COLOR, fg="#aaaacc",
                  activebackground=NAV_ACTIVE,
                  font=FONT_NAV, relief="flat",
                  command=self._prev_preset).grid(row=0, column=0, padx=(6, 10))

        # Pulsanti 1-4
        for i in range(4):
            btn = tk.Button(
                nav_frame,
                text=str(i + 1),
                width=5, height=2,
                bg=OFF_COLOR, fg="#ffffff",
                activebackground=PRESET_COLORS[i],
                font=FONT_PRESET,
                relief="flat",
                command=lambda n=i: self._select_preset(n),
            )
            btn.grid(row=0, column=i + 1, padx=5)
            self._preset_btns.append(btn)

        # ► NEXT
        tk.Button(nav_frame, text="►", width=4, height=2,
                  bg=NAV_COLOR, fg="#aaaacc",
                  activebackground=NAV_ACTIVE,
                  font=FONT_NAV, relief="flat",
                  command=self._next_preset).grid(row=0, column=5, padx=(10, 6))

        # ── FX TOGGLE ────────────────────────────────────────────────── #
        fx_frame = tk.Frame(self.root, bg=BG, pady=2)
        fx_frame.pack()

        tk.Label(fx_frame, text="EFFETTI", bg=BG,
                 fg="#666677", font=FONT_STATUS).grid(
            row=0, column=0, columnspan=4, pady=(0, 2))

        for col, (label, key) in enumerate(
                [("DRIVE", "drive"), ("MOD", "mod"),
                 ("DELAY", "delay"), ("REVERB", "reverb")]):
            btn = tk.Button(
                fx_frame, text=label, width=7, height=2,
                bg=OFF_COLOR, fg="#ffffff",
                activebackground=ON_COLOR,
                font=FONT_FX, relief="flat",
                command=lambda k=key: self._toggle_fx(k),
            )
            btn.grid(row=1, column=col, padx=6, pady=2)
            self._fx_btns[key] = btn

        # ── HINT ─────────────────────────────────────────────────────── #
        tk.Label(self.root,
                 text="1-4: preset   ←→: prev/next   D M E R: fx",
                 bg=BG, fg="#444455", font=FONT_STATUS).pack(pady=(4, 8))

    def _bind_keys(self):
        for i in range(4):
            self.root.bind(str(i + 1), lambda e, n=i: self._select_preset(n))
        self.root.bind("d", lambda e: self._toggle_fx("drive"))
        self.root.bind("m", lambda e: self._toggle_fx("mod"))
        self.root.bind("e", lambda e: self._toggle_fx("delay"))
        self.root.bind("r", lambda e: self._toggle_fx("reverb"))
        # KeyRelease fires once per press (no autorepeat) — prevents flood of requests
        self.root.bind("<KeyRelease-Left>",  lambda e: self._prev_preset())
        self.root.bind("<KeyRelease-Right>", lambda e: self._next_preset())

    # ------------------------------------------------------------------ #
    #  Actions → server
    # ------------------------------------------------------------------ #

    def _select_preset(self, index):
        try:
            sio.emit("change_preset", {"preset": index})
        except Exception:
            pass
        self._update_preset_buttons(index)

    def _prev_preset(self):
        self._select_preset((self._preset - 1) % 4)

    def _next_preset(self):
        self._select_preset((self._preset + 1) % 4)

    def _toggle_fx(self, key):
        try:
            sio.emit("toggle_effect_onoff", {"effect_type": key})
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    #  Display helpers
    # ------------------------------------------------------------------ #

    def _update_preset_buttons(self, index):
        self._preset = index
        self._disp_num.configure(text=str(index + 1))
        for i, btn in enumerate(self._preset_btns):
            btn.configure(bg=PRESET_COLORS[i] if i == index else OFF_COLOR)

    def _update_name_bpm(self, name, bpm):
        self._disp_name.configure(text=name)
        self._disp_bpm.configure(text=f"{bpm} BPM")

    def _update_fx(self, key, is_on):
        self._fx_state[key] = is_on
        btn = self._fx_btns.get(key)
        if btn:
            btn.configure(bg=ON_COLOR if is_on else OFF_COLOR)
        lbl = self._disp_fx.get(key)
        if lbl:
            lbl.configure(fg=LCD_GREEN if is_on else LCD_DIM)

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
            name   = data.get("Name", "")
            bpm    = data.get("BPM", "0")
            self.root.after(0, self._update_preset_buttons, preset)
            self.root.after(0, self._update_name_bpm, name, bpm)
            for key in ("drive", "mod", "delay", "reverb"):
                state = data.get(key, "Off") == "On"
                self.root.after(0, self._update_fx, key, state)

        @sio.on(EVT_UPDATE_ONOFF)
        def on_update_onoff(data):
            key   = data.get("effect_type", "")
            state = data.get("state", "Off") == "On"
            if key in self._fx_btns:
                self.root.after(0, self._update_fx, key, state)

        @sio.on(EVT_UPDATE_PRESET)
        def on_update_preset(data):
            preset = data.get("value", 0)
            self.root.after(0, self._update_preset_buttons, preset)

        @sio.on(EVT_CONN_MSG)
        def on_conn_msg(data):
            self.root.after(0, self._set_status, data.get("message", ""))

        @sio.on(EVT_CONN_LOST)
        def on_conn_lost(data):
            self.root.after(0, self._set_status, "Connessione amp persa")

        import threading
        def try_connect():
            try:
                sio.connect(SERVER_URL)
            except Exception as e:
                self.root.after(0, self._set_status,
                                f"Server non raggiungibile: {e}")

        threading.Thread(target=try_connect, daemon=True).start()


def main():
    root = tk.Tk()
    PedalUI(root)
    root.mainloop()
    sio.disconnect()


if __name__ == "__main__":
    main()
