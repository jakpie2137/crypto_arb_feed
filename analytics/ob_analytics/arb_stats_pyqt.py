import os
import time
import traceback
from bisect import bisect_left

try:
    import psycopg
    _pg_connect = psycopg.connect
    _pg_operational_error = psycopg.OperationalError
    _PG3 = True  # psycopg3 handles Polish/cp1250 server errors
except ImportError:
    import psycopg2 as _pg
    _pg_connect = _pg.connect
    _pg_operational_error = _pg.OperationalError
    _PG3 = False
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

from pyqtgraph.Qt import QtWidgets, QtCore, QtGui
import pyqtgraph as pg

# ============= CONFIG =============

DB_HOST = os.getenv("PG_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("PG_PORT", "2137"))
DB_NAME = os.getenv("PG_DB", "arb_test")
DB_USER = os.getenv("PG_USER", "postgres")
DB_PASS = os.getenv("PG_PASS", "postgres")

STATS_TABLE = "market_data.ob_arb_stats_historic"
DEFAULT_LOOKBACK_HOURS = 8  # fallback gdy load_stats() wywołane bez start_dt
STATS_MAX_ROWS = 10_000_000

BACKGROUND = (28, 28, 28)
TEXT_COLOR = (220, 220, 220)

RANK_CONFIG = [
    (1, "total_positive_gain_usd", "Rank1: total_positive_gain_usd"),
    (2, "first_level_gain_usd", "Rank2: first_level_gain_usd"),
    (3, "vwap_gain_usd_for_target_volume", "Rank3: vwap_gain_USD_for_target_volume"),
    (4, "gain_pct_at_target_gain", "Rank4: gain_pct_at_target_gain"),
]

# Performance knobs
ENABLE_TOPN_INSTRUMENTS = True
TOPN_INSTRUMENTS = 60           # per (rank, side); the rest collapses into "OTHER"
INCLUDE_OTHER_BUCKET = True
OTHER_LABEL = "__OTHER__"

# szeroko rozstrzelona paleta kolorów (różne odcienie, nie tylko ciepłe)
BASE_COLORS = [
    "#ff595e", "#ffca3a", "#8ac926", "#1982c4", "#6a4c93",
    "#f72585", "#4cc9f0", "#7209b7", "#b5179e", "#560bad",
    "#3a0ca3", "#4895ef", "#06d6a0", "#ffd166", "#ef476f",
    "#118ab2", "#073b4c", "#f9844a", "#90be6d", "#577590",
    "#ff9f1c", "#2ec4b6", "#e71d36", "#bc6c25", "#ff99c8",
]


# ============= DB =============

def get_pg_connection():
    # Avoid libpq reading pgpass from %APPDATA% (can cause UnicodeDecodeError on Windows)
    old_pgpass = os.environ.pop("PGPASSFILE", None)
    try:
        os.environ["PGPASSFILE"] = "C:/.pgpass_disabled"
        if _PG3:
            # psycopg3: client_encoding helps decode pre-connection errors (Polish/cp1250)
            conninfo = (
                f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
                f"user={DB_USER} password={DB_PASS} "
                "client_encoding=windows-1250"
            )
            return _pg_connect(conninfo)
        else:
            old_enc = os.environ.pop("PGCLIENTENCODING", None)
            try:
                os.environ["PGCLIENTENCODING"] = "UTF8"
                return _pg_connect(
                    host=str(DB_HOST), port=str(DB_PORT),
                    dbname=str(DB_NAME), user=str(DB_USER), password=str(DB_PASS),
                )
            except UnicodeDecodeError as e:
                raise _pg_operational_error(
                    "Connection failed. PostgreSQL uses non-UTF-8 locale; psycopg2 cannot decode. "
                    "Try: pip install 'psycopg[binary]' (psycopg3 handles this). "
                    "Or set lc_messages='en_US.UTF-8' in postgresql.conf."
                ) from e
            finally:
                if old_enc is not None:
                    os.environ["PGCLIENTENCODING"] = old_enc
                elif "PGCLIENTENCODING" in os.environ:
                    del os.environ["PGCLIENTENCODING"]
    finally:
        if old_pgpass is not None:
            os.environ["PGPASSFILE"] = old_pgpass
        elif "PGPASSFILE" in os.environ:
            del os.environ["PGPASSFILE"]


def _utcnow_naive():
    """Timezone-aware now in UTC, returned as naive datetime for compatibility with existing UI/DB code."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def load_stats(start_dt=None, exchange=None, symbol=None):
    """
    Load stats from DB.
    start_dt: naive datetime. If None, we enforce default now()-DEFAULT_LOOKBACK_HOURS.
    Optional filters: exchange (src_exchange) and symbol.
    vwap_gain_usd_for_target_volume is derived in Python from:
      vwap_gain_pct_for_target_volume * target_volume_quote / 100.0
    assuming pct is stored as percentage (e.g. 0.5 = 0.5%).
    """
    if start_dt is None:
        start_dt = _utcnow_naive() - timedelta(hours=DEFAULT_LOOKBACK_HOURS)

    where_clauses = [f"ts >= '{start_dt.strftime('%Y-%m-%d %H:%M:%S')}'"]
    if exchange:
        where_clauses.append(f"src_exchange = '{exchange}'")
    if symbol:
        where_clauses.append(f"symbol = '{symbol}'")
    where_sql = " AND ".join(where_clauses)

    query = f"""
        SELECT
            ts,
            symbol,
            src_exchange,
            side,
            rank_type,
            rank_position,
            first_level_gain_pct,
            first_level_gain_usd,
            first_level_volume_quote,
            total_positive_gain_usd,
            total_volume_quote,
            avg_gain_pct_total,
            target_volume_quote,
            vwap_gain_pct_for_target_volume,
            target_gain_usd,
            volume_quote_to_reach_target_gain,
            gain_pct_at_target_gain
        FROM {STATS_TABLE}
        WHERE {where_sql}
        ORDER BY ts ASC
        LIMIT {STATS_MAX_ROWS};
    """
    t0 = time.perf_counter()
    with get_pg_connection() as conn:
        df = pd.read_sql(query, conn)
    t1 = time.perf_counter()
    print(f"[DB] Loaded {len(df)} rows in {t1 - t0:.2f}s (where: {where_sql})")

    if df.empty:
        return df

    df["instrument"] = df["src_exchange"] + "-" + df["symbol"]
    df["ts"] = pd.to_datetime(df["ts"])
    df["ts_num"] = df["ts"].astype("int64") // 10**9  # seconds since epoch
    df["rank_type_int"] = pd.to_numeric(df["rank_type"], errors="coerce").astype("Int64")

    # --- derive vwap_gain_usd_for_target_volume ---
    for col in ["vwap_gain_pct_for_target_volume", "target_volume_quote"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["vwap_gain_usd_for_target_volume"] = (
        df["vwap_gain_pct_for_target_volume"] * df["target_volume_quote"] / 100.0
    )

    return df


def get_latest_ts_in_db(start_dt, exchange=None, symbol=None):
    """
    Return max(ts) from stats table for ts >= start_dt (and optional filters), or None.
    Used for smart Refresh so we don't scan entire history.
    """
    where_clauses = [f"ts >= '{start_dt.strftime('%Y-%m-%d %H:%M:%S')}'"]
    if exchange:
        where_clauses.append(f"src_exchange = '{exchange}'")
    if symbol:
        where_clauses.append(f"symbol = '{symbol}'")
    where_sql = " AND ".join(where_clauses)
    query = f"SELECT max(ts) AS max_ts FROM {STATS_TABLE} WHERE {where_sql};"
    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
    return row[0] if row and row[0] is not None else None


# ============= UTIL / AXIS =============

class DateAxis(pg.AxisItem):
    def tickStrings(self, values, scale, spacing):
        return [datetime.fromtimestamp(v).strftime("%H:%M:%S") for v in values]


def make_plot_widget(title):
    axis = DateAxis(orientation="bottom")
    pw = pg.PlotWidget(axisItems={"bottom": axis})
    pw.setBackground(BACKGROUND)
    pw.showGrid(x=True, y=True, alpha=0.3)
    pw.getAxis("left").setPen(TEXT_COLOR)
    pw.getAxis("bottom").setPen(TEXT_COLOR)
    pw.getAxis("left").setTextPen(TEXT_COLOR)
    pw.getAxis("bottom").setTextPen(TEXT_COLOR)
    pw.setTitle(title, color=TEXT_COLOR)
    return pw


class CustomTooltip(QtWidgets.QLabel):
    """
    Own tooltip widget so we fully control background/text color.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowFlags(QtCore.Qt.ToolTip)
        self.setAttribute(QtCore.Qt.WA_TransparentForMouseEvents)
        self.setTextFormat(QtCore.Qt.PlainText)
        self.setMargin(4)
        font = self.font()
        font.setPointSize(font.pointSize() + 5)
        self.setFont(font)
        self.hide()

    def show_tooltip(self, text, bg_color: QtGui.QColor, text_color: QtGui.QColor, global_pos: QtCore.QPoint):
        self.setText(text)
        r, g, b, a = bg_color.red(), bg_color.green(), bg_color.blue(), bg_color.alpha()
        tr, tg, tb = text_color.red(), text_color.green(), text_color.blue()
        self.setStyleSheet(
            "QLabel {"
            f"background-color: rgba({r}, {g}, {b}, {a});"
            f"color: rgb({tr}, {tg}, {tb});"
            "border: 1px solid rgb(220,220,220);"
            "border-radius: 3px;"
            "}"
        )
        self.adjustSize()
        offset = QtCore.QPoint(15, 15)
        self.move(global_pos + offset)
        self.show()


# ============= MAIN WINDOW =============

class StatsWindow(QtWidgets.QMainWindow):
    def __init__(self, df, start_dt, exchange_filter=None, symbol_filter=None):
        super().__init__()
        self.df = df
        self.current_start_dt = start_dt
        self.current_exchange = exchange_filter
        self.current_symbol = symbol_filter

        self.setWindowTitle("ARB Stats – PyQtGraph")
        self.resize(1500, 900)

        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        vlayout = QtWidgets.QVBoxLayout()
        central.setLayout(vlayout)

        # --- Toolbar ---
        toolbar = QtWidgets.QHBoxLayout()
        vlayout.addLayout(toolbar)

        self.btn_autoscale = QtWidgets.QPushButton("Autoscale")
        self.btn_zoom_normal = QtWidgets.QPushButton("Normal zoom")
        self.btn_zoom_x = QtWidgets.QPushButton("Zoom X")
        self.btn_zoom_y = QtWidgets.QPushButton("Zoom Y")
        self.btn_rect_zoom = QtWidgets.QPushButton("Rectangle zoom")

        for btn in [
            self.btn_autoscale,
            self.btn_zoom_normal,
            self.btn_zoom_x,
            self.btn_zoom_y,
            self.btn_rect_zoom,
        ]:
            btn.setStyleSheet("background-color: #333333; color: white;")
            toolbar.addWidget(btn)

        # start date filter
        self.start_date_edit = QtWidgets.QDateTimeEdit()
        self.start_date_edit.setDisplayFormat("yyyy-MM-dd HH:mm")
        self.start_date_edit.setCalendarPopup(True)
        self.start_date_edit.setMinimumWidth(150)
        qdt_default = QtCore.QDateTime(start_dt)
        self.start_date_edit.setDateTime(qdt_default)
        toolbar.addSpacing(8)
        start_label = QtWidgets.QLabel("Start:")
        start_label.setStyleSheet("color: white;")
        toolbar.addWidget(start_label)
        toolbar.addWidget(self.start_date_edit)

        # Quick range buttons
        self.quick_range_group = QtWidgets.QButtonGroup(self)
        self.quick_buttons = []
        for hours in [2, 4, 6, 8]:
            btn = QtWidgets.QPushButton(f"{hours}h")
            btn.setCheckable(True)
            if hours == 2:
                btn.setChecked(True)
            btn.setStyleSheet("background-color: #333333; color: white;")
            toolbar.addWidget(btn)
            self.quick_range_group.addButton(btn, hours)
            self.quick_buttons.append(btn)
            btn.clicked.connect(lambda checked, h=hours: self.on_quick_range_clicked(h))

        # exchange filter
        toolbar.addSpacing(8)
        exch_label = QtWidgets.QLabel("Exch:")
        exch_label.setStyleSheet("color: white;")
        toolbar.addWidget(exch_label)
        self.exchange_combo = QtWidgets.QComboBox()
        self.exchange_combo.setEditable(True)
        self.exchange_combo.setMinimumWidth(130)
        toolbar.addWidget(self.exchange_combo)

        # symbol filter
        toolbar.addSpacing(8)
        sym_label = QtWidgets.QLabel("Symbol:")
        sym_label.setStyleSheet("color: white;")
        toolbar.addWidget(sym_label)
        self.symbol_combo = QtWidgets.QComboBox()
        self.symbol_combo.setEditable(True)
        self.symbol_combo.setMinimumWidth(130)
        toolbar.addWidget(self.symbol_combo)

        toolbar.addStretch(1)
        self.btn_refresh = QtWidgets.QPushButton("Refresh")
        self.btn_refresh.setStyleSheet("background-color: #444444; color: white;")
        toolbar.addWidget(self.btn_refresh)

        # --- Main content area ---
        main_layout = QtWidgets.QHBoxLayout()
        vlayout.addLayout(main_layout, 1)

        self.plots_layout = QtWidgets.QVBoxLayout()
        main_layout.addLayout(self.plots_layout, 1)

        self.legend_widget = QtWidgets.QListWidget()
        self.legend_widget.setMinimumWidth(220)
        self.legend_widget.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        self.legend_widget.setVerticalScrollMode(QtWidgets.QAbstractItemView.ScrollPerPixel)
        self.legend_widget.setFocusPolicy(QtCore.Qt.NoFocus)
        self.legend_widget.setStyleSheet(
            "QListWidget {"
            "background-color: #202020;"
            "color: white;"
            "font-size: 11px;"
            "}"
        )
        self.legend_widget.setSelectionMode(QtWidgets.QAbstractItemView.MultiSelection)
        main_layout.addWidget(self.legend_widget)

        # --- Plots ---
        self.plots = []
        for _, _, title in RANK_CONFIG:
            pw = make_plot_widget(title)
            self.plots_layout.addWidget(pw, 1)
            self.plots.append(pw)

        for pw in self.plots[1:]:
            pw.setXLink(self.plots[0])

        # segment index for fast hover:
        # per plot: dict[ts_int] -> list[segments], plus sorted ts list for nearest lookup
        self.segment_data = [dict() for _ in self.plots]
        self.segment_ts_sorted = [[] for _ in self.plots]

        self.color_map = {}          # instrument -> QColor
        self.used_instruments = set()
        self.plot_bars_by_instrument = [dict() for _ in self.plots]
        self.instrument_rank1_max_height = {}
        self.legend_items = {}
        self.visible_instruments = set()

        # separators removed for performance; keep for API compatibility
        self.separator_items = [[] for _ in self.plots]

        self.tooltip = CustomTooltip(self)

        self._populate_filter_combos(df)
        self._populate_plots()
        self._build_legend()

        for idx, pw in enumerate(self.plots):
            pw.scene().sigMouseMoved.connect(
                lambda pos, i=idx: self.on_mouse_moved(i, pos)
            )

        self.btn_autoscale.clicked.connect(self.on_autoscale)
        self.btn_zoom_normal.clicked.connect(self.on_zoom_normal)
        self.btn_zoom_x.clicked.connect(self.on_zoom_x)
        self.btn_zoom_y.clicked.connect(self.on_zoom_y)
        self.btn_rect_zoom.clicked.connect(self.on_rect_zoom)
        self.btn_refresh.clicked.connect(self.on_refresh)

        # legend interactions (Plotly-like)
        self.legend_widget.itemClicked.connect(self.on_legend_item_clicked)
        self.legend_widget.itemDoubleClicked.connect(self.on_legend_item_double_clicked)

    # ---------- FILTERS UI ----------

    def _populate_filter_combos(self, df):
        if df is None or df.empty or "src_exchange" not in df.columns or "symbol" not in df.columns:
            self.exchange_combo.blockSignals(True)
            self.exchange_combo.clear()
            self.exchange_combo.addItem("All")
            self.exchange_combo.blockSignals(False)

            self.symbol_combo.blockSignals(True)
            self.symbol_combo.clear()
            self.symbol_combo.addItem("All")
            self.symbol_combo.blockSignals(False)
            return

        exchanges = sorted(set(df["src_exchange"].dropna().astype(str).tolist()))
        self.exchange_combo.blockSignals(True)
        self.exchange_combo.clear()
        self.exchange_combo.addItem("All")
        for ex in exchanges:
            self.exchange_combo.addItem(ex)
        if self.current_exchange and self.current_exchange in exchanges:
            idx = self.exchange_combo.findText(self.current_exchange)
            if idx >= 0:
                self.exchange_combo.setCurrentIndex(idx)
        else:
            self.exchange_combo.setCurrentIndex(0)
        exch_compl = QtWidgets.QCompleter(exchanges)
        exch_compl.setCaseSensitivity(QtCore.Qt.CaseInsensitive)
        self.exchange_combo.setCompleter(exch_compl)
        self.exchange_combo.blockSignals(False)

        symbols = sorted(set(df["symbol"].dropna().astype(str).tolist()))
        self.symbol_combo.blockSignals(True)
        self.symbol_combo.clear()
        self.symbol_combo.addItem("All")
        for s in symbols:
            self.symbol_combo.addItem(s)
        if self.current_symbol and self.current_symbol in symbols:
            idx = self.symbol_combo.findText(self.current_symbol)
            if idx >= 0:
                self.symbol_combo.setCurrentIndex(idx)
        else:
            self.symbol_combo.setCurrentIndex(0)
        sym_compl = QtWidgets.QCompleter(symbols)
        sym_compl.setCaseSensitivity(QtCore.Qt.CaseInsensitive)
        self.symbol_combo.setCompleter(sym_compl)
        self.symbol_combo.blockSignals(False)

    # ---------- HELPERS FOR TOOLTIP / LEGEND ----------

    def _fmt_val(self, v):
        # keep NaN logic
        try:
            if pd.isna(v):
                return "nan"
        except Exception:
            pass

        try:
            x = float(v)
        except Exception:
            return str(v)

        # --- CUSTOM FORMAT RULE FOR VOLUME ---
        # Use plain formatting if < 8 digits (i.e. < 10,000,000)
        # Use scientific notation ONLY for 8+ digit numbers
        if abs(x) < 10_000_000:
            # normal formatting, trimmed
            if abs(x) >= 1000:
                return f"{x:,.0f}"  # 1,7614 → "1,761" (thousands)
            else:
                return f"{x:.4g}"
        else:
            # fallback sci notation for truly big numbers
            return f"{x:.4e}"

    def _build_tooltip(self, rank_type, row):
        ts_str = row["ts"].strftime("%Y-%m-%d %H:%M:%S")
        inst = row["instrument"]
        side = row["side"]

        if rank_type == 1:
            return (
                f"{ts_str}\n"
                f"{inst} {side}\n"
                f"total_positive_gain_usd = {self._fmt_val(row['total_positive_gain_usd'])}\n"
                f"total_volume_quote = {self._fmt_val(row['total_volume_quote'])}\n"
                f"avg_gain_pct_total = {self._fmt_val(row['avg_gain_pct_total'])}"
            )
        elif rank_type == 2:
            return (
                f"{ts_str}\n"
                f"{inst} {side}\n"
                f"first_level_gain_usd = {self._fmt_val(row['first_level_gain_usd'])}\n"
                f"first_level_gain_pct = {self._fmt_val(row['first_level_gain_pct'])}\n"
                f"first_level_volume_quote = {self._fmt_val(row['first_level_volume_quote'])}"
            )
        elif rank_type == 3:
            return (
                f"{ts_str}\n"
                f"{inst} {side}\n"
                f"target_volume_quote = {self._fmt_val(row['target_volume_quote'])}\n"
                f"vwap_gain_usd_for_target_volume = {self._fmt_val(row['vwap_gain_usd_for_target_volume'])}\n"
                f"vwap_gain_pct_for_target_volume = {self._fmt_val(row['vwap_gain_pct_for_target_volume'])}"
            )
        elif rank_type == 4:
            return (
                f"{ts_str}\n"
                f"{inst} {side}\n"
                f"target_gain_usd = {self._fmt_val(row['target_gain_usd'])}\n"
                f"volume_quote_to_reach_target_gain = {self._fmt_val(row['volume_quote_to_reach_target_gain'])}\n"
                f"gain_pct_at_target_gain = {self._fmt_val(row['gain_pct_at_target_gain'])}"
            )

        return f"{ts_str}\n{inst} {side}"

    def _build_legend(self):
        self.legend_widget.clear()
        self.legend_items.clear()

        if not self.used_instruments:
            return

        def sort_key(inst):
            h = self.instrument_rank1_max_height.get(inst, 0.0)
            return (-h, inst)

        for inst in sorted(self.used_instruments, key=sort_key):
            item = QtWidgets.QListWidgetItem(inst)
            color = self.color_map.get(inst, QtGui.QColor(255, 255, 255))

            pixmap = QtGui.QPixmap(14, 14)
            pixmap.fill(color)
            icon = QtGui.QIcon(pixmap)
            item.setIcon(icon)

            self.legend_widget.addItem(item)
            self.legend_items[inst] = item

        # domyślnie – wszyscy widoczni
        if not self.visible_instruments:
            self.visible_instruments = set(self.used_instruments)

        self._sync_legend_selection_from_visible()

    def _get_color_for_instrument(self, inst):
        if inst not in self.color_map:
            if inst == OTHER_LABEL:
                self.color_map[inst] = QtGui.QColor(140, 140, 140)
                return self.color_map[inst]

            idx = len(self.color_map)
            base_hex = BASE_COLORS[idx % len(BASE_COLORS)]
            color = QtGui.QColor(base_hex)
            if idx >= len(BASE_COLORS):
                factor = 0.75 if (idx // len(BASE_COLORS)) % 2 == 0 else 1.05
                r = max(0, min(int(color.red() * factor), 255))
                g = max(0, min(int(color.green() * factor), 255))
                b = max(0, min(int(color.blue() * factor), 255))
                color = QtGui.QColor(r, g, b)
            self.color_map[inst] = color
        return self.color_map[inst]

    def _selected_start_dt(self):
        qdt = self.start_date_edit.dateTime()
        return qdt.toPyDateTime().replace(tzinfo=None)

    # ---------- QUICK RANGE HANDLER ----------

    def on_quick_range_clicked(self, hours):
        now_utc = _utcnow_naive()
        new_start = now_utc - timedelta(hours=hours)
        qdt = QtCore.QDateTime(new_start)
        self.start_date_edit.setDateTime(qdt)

    # ---------- TOOLTIP HITTEST (FAST) ----------

    @staticmethod
    def _nearest_ts(sorted_ts, x):
        """Return nearest int timestamp from a sorted list[int] for float x; or None."""
        if not sorted_ts:
            return None
        xi = int(round(float(x)))
        pos = bisect_left(sorted_ts, xi)
        if pos <= 0:
            return sorted_ts[0]
        if pos >= len(sorted_ts):
            return sorted_ts[-1]
        before = sorted_ts[pos - 1]
        after = sorted_ts[pos]
        return before if abs(xi - before) <= abs(after - xi) else after

    def on_mouse_moved(self, plot_idx, pos):
        if plot_idx < 0 or plot_idx >= len(self.plots):
            return

        seg_by_ts = self.segment_data[plot_idx]
        ts_sorted = self.segment_ts_sorted[plot_idx]
        if not seg_by_ts or not ts_sorted:
            self.tooltip.hide()
            return

        pw = self.plots[plot_idx]
        vb = pw.getViewBox()
        mouse_point = vb.mapSceneToView(pos)
        x = mouse_point.x()
        y = mouse_point.y()

        ts_int = self._nearest_ts(ts_sorted, x)
        if ts_int is None:
            self.tooltip.hide()
            return

        segments = seg_by_ts.get(ts_int)
        if not segments:
            self.tooltip.hide()
            return

        hit = None
        best_dx = None

        for seg in segments:
            if y < seg["y_bottom"] or y > seg["y_top"]:
                continue
            dx = abs(x - seg["x_center"])
            if dx <= seg["width"] / 2.0:
                if best_dx is None or dx < best_dx:
                    best_dx = dx
                    hit = seg

        if hit is None:
            self.tooltip.hide()
            return

        text = hit.get("tooltip", "")
        if not text:
            self.tooltip.hide()
            return

        color = hit.get("color")
        if isinstance(color, QtGui.QColor):
            bg = QtGui.QColor(color)
            bg.setAlpha(int(255 * 0.55))

            r, g, b = color.red(), color.green(), color.blue()
            luminance = 0.299 * r + 0.587 * g + 0.114 * b
            text_color = QtGui.QColor(0, 0, 0) if luminance > 170 else QtGui.QColor(255, 255, 255)
        else:
            bg = QtGui.QColor(0, 0, 0, 200)
            text_color = QtGui.QColor(255, 255, 255)

        self.tooltip.show_tooltip(text, bg, text_color, QtGui.QCursor.pos())

    # ---------- LEGEND / FILTER (Plotly-like) ----------

    def on_legend_item_clicked(self, item: QtWidgets.QListWidgetItem):
        inst = item.text()
        if inst not in self.used_instruments:
            return
        all_inst = set(self.used_instruments)

        # unfiltered -> single click ukrywa trace
        if not self.visible_instruments or self.visible_instruments == all_inst:
            self.visible_instruments = set(all_inst)
            if inst in self.visible_instruments:
                self.visible_instruments.remove(inst)
        else:
            # filtered -> single click toggluje danego trace'a
            if inst in self.visible_instruments:
                self.visible_instruments.remove(inst)
            else:
                self.visible_instruments.add(inst)

        if not self.visible_instruments:
            # jak ktoś się zapomni i wyklika wszystko – wracamy do all
            self.visible_instruments = set(all_inst)

        self._apply_legend_filter()
        self._sync_legend_selection_from_visible()

    def on_legend_item_double_clicked(self, item: QtWidgets.QListWidgetItem):
        inst = item.text()
        all_inst = set(self.used_instruments)
        if not all_inst:
            return

        # unfiltered -> double click = tylko ten instrument
        if not self.visible_instruments or self.visible_instruments == all_inst:
            self.visible_instruments = {inst}
        else:
            # filtered -> double click = pokaż wszystkich
            self.visible_instruments = set(all_inst)

        self._apply_legend_filter()
        self._sync_legend_selection_from_visible()

    def _apply_legend_filter(self):
        visible = self.visible_instruments or set(self.used_instruments)
        all_inst = set(self.used_instruments)

        # bary
        for bars_by_inst in self.plot_bars_by_instrument:
            for inst, bars in bars_by_inst.items():
                vis = inst in visible
                for bar in bars:
                    bar.setVisible(vis)

    def _sync_legend_selection_from_visible(self):
        visible = self.visible_instruments or set(self.used_instruments)
        self.legend_widget.blockSignals(True)
        for inst, item in self.legend_items.items():
            item.setSelected(inst in visible)
        self.legend_widget.blockSignals(False)

    # ---------- PLOTTING ----------

    def _populate_plots(self):
        df = self.df
        bar_width = 25

        # reset caches
        self.segment_data = [dict() for _ in self.plots]
        self.segment_ts_sorted = [[] for _ in self.plots]
        self.plot_bars_by_instrument = [dict() for _ in self.plots]
        self.instrument_rank1_max_height.clear()
        self.used_instruments.clear()
        self.separator_items = [[] for _ in self.plots]

        if df is None or df.empty or "rank_type_int" not in df.columns:
            for idx, (_, _, ylabel) in enumerate(RANK_CONFIG):
                pw = self.plots[idx]
                pw.clear()
                pw.setLabel("left", ylabel)
                pw.setLabel("bottom", "time")
            return

        for idx, (rank_type_val, col, ylabel) in enumerate(RANK_CONFIG):
            pw = self.plots[idx]
            pw.clear()

            seg_by_ts = self.segment_data[idx]
            bars_by_inst = self.plot_bars_by_instrument[idx]

            sub = df[df["rank_type_int"] == rank_type_val].copy()
            if sub.empty:
                pw.setLabel("left", ylabel)
                pw.setLabel("bottom", "time")
                continue

            sub[col] = pd.to_numeric(sub[col], errors="coerce")
            sub = sub.dropna(subset=[col])
            if sub.empty:
                pw.setLabel("left", ylabel)
                pw.setLabel("bottom", "time")
                continue

            rank_has_data = False

            for side, sign in [("BID", 1), ("ASK", -1)]:
                side_sub = sub[sub["side"] == side].copy()
                if side_sub.empty:
                    continue

                # Precompute tooltip lookup ONCE (kills the inner pandas filter bottleneck)
                # key: (ts_num:int, instrument:str) -> tooltip:str
                # Use itertuples for speed.
                tooltip_map = {}
                try:
                    for r in side_sub.itertuples(index=False):
                        # Access by attribute names (same as columns)
                        # ts_num is integer-ish
                        tsn = int(getattr(r, "ts_num"))
                        inst = getattr(r, "instrument")
                        # convert tuple row to dict-like via _asdict is expensive; instead use pandas row access once:
                        # we can map back by locating the row in side_sub using a lightweight view:
                        # BUT easiest: use side_sub.loc[...] is also expensive.
                        # Instead: build tooltip via Series-style using side_sub columns -> keep as DataFrame row by position:
                        # We'll fill below using enumerate over itertuples with index.
                except Exception:
                    tooltip_map = {}

                # Efficient: build using numpy arrays + DataFrame rows by integer position
                # (still O(n) once, not O(n*m))
                if side_sub is not None and not side_sub.empty:
                    # to avoid attribute issues, do once with iterrows (still OK: <= rows visible on screen)
                    for _, row in side_sub.iterrows():
                        tooltip_map[(int(row["ts_num"]), row["instrument"])] = self._build_tooltip(rank_type_val, row)

                side_sub["value"] = pd.to_numeric(side_sub[col], errors="coerce") * sign
                side_sub = side_sub.dropna(subset=["value"])

                if side_sub.empty:
                    continue

                # pivot to time x instrument
                piv = side_sub.pivot_table(
                    index="ts_num", columns="instrument", values="value", aggfunc="sum"
                ).fillna(0.0)

                if piv.empty:
                    continue

                # TOP-N reduction (optional): massively reduces QGraphicsItems & legend bloat
                if ENABLE_TOPN_INSTRUMENTS and piv.shape[1] > TOPN_INSTRUMENTS:
                    totals = np.abs(piv.values).sum(axis=0)
                    if totals.size > 0:
                        top_idx = np.argpartition(-totals, TOPN_INSTRUMENTS - 1)[:TOPN_INSTRUMENTS]
                        top_cols = piv.columns[top_idx]
                        piv_top = piv[top_cols].copy()
                        if INCLUDE_OTHER_BUCKET:
                            other_cols = [c for c in piv.columns if c not in set(top_cols)]
                            if other_cols:
                                piv_top[OTHER_LABEL] = piv[other_cols].sum(axis=1).values
                        piv = piv_top

                rank_has_data = True

                instruments = list(piv.columns)
                x_vals = piv.index.values.astype(float)

                cum = np.zeros_like(x_vals, dtype=float)
                for inst in instruments:
                    vals = piv[inst].values.astype(float)
                    if np.all(vals == 0):
                        continue

                    self.used_instruments.add(inst)
                    color = self._get_color_for_instrument(inst)

                    if rank_type_val == 1 and inst != OTHER_LABEL:
                        try:
                            max_val = float(np.nanmax(np.abs(vals)))
                            if max_val > 0:
                                prev = self.instrument_rank1_max_height.get(inst, 0.0)
                                if max_val > prev:
                                    self.instrument_rank1_max_height[inst] = max_val
                        except Exception:
                            pass

                    y0_arr = cum.copy()
                    h_arr = vals
                    cum = cum + vals

                    bar = pg.BarGraphItem(
                        x=x_vals,
                        height=h_arr,
                        width=bar_width,
                        y0=y0_arr,
                        brush=color,
                        pen=None,
                    )
                    pw.addItem(bar)
                    bars_by_inst.setdefault(inst, []).append(bar)

                    # segments for hover, binned by timestamp
                    for j, (xv, val) in enumerate(zip(x_vals, vals)):
                        if val == 0:
                            continue
                        y0 = y0_arr[j]
                        y1 = y0 + val
                        ts_int = int(xv)

                        tip = tooltip_map.get((ts_int, inst))
                        if not tip:
                            ts = datetime.fromtimestamp(float(xv))
                            tip = (
                                f"{ts.strftime('%Y-%m-%d %H:%M:%S')}\n"
                                f"{inst} {side}\n"
                                f"{col} = {val:.6g}"
                            )

                        seg = {
                            "x_center": xv,
                            "y_bottom": min(y0, y1),
                            "y_top": max(y0, y1),
                            "width": bar_width,
                            "tooltip": tip,
                            "color": color,
                        }
                        seg_by_ts.setdefault(ts_int, []).append(seg)

            if rank_has_data:
                pw.addLine(
                    y=0,
                    pen=pg.mkPen(200, 200, 200, 120, width=2),
                )

            # cache sorted timestamps for fast nearest lookup
            ts_sorted = sorted(seg_by_ts.keys())
            self.segment_ts_sorted[idx] = ts_sorted

            pw.setLabel("left", ylabel)
            pw.setLabel("bottom", "time")

        # domyślnie – wszyscy widoczni
        self.visible_instruments = set(self.used_instruments)
        self._apply_legend_filter()

    # ---------- BUTTON HANDLERS ----------

    def on_autoscale(self):
        for pw in self.plots:
            pw.enableAutoRange(x=True, y=True)

    def on_zoom_normal(self):
        for pw in self.plots:
            vb = pw.getViewBox()
            vb.setMouseMode(pg.ViewBox.PanMode)
            vb.setMouseEnabled(x=True, y=True)

    def on_zoom_x(self):
        for pw in self.plots:
            vb = pw.getViewBox()
            vb.setMouseMode(pg.ViewBox.PanMode)
            vb.setMouseEnabled(x=True, y=False)

    def on_zoom_y(self):
        for pw in self.plots:
            vb = pw.getViewBox()
            vb.setMouseMode(pg.ViewBox.PanMode)
            vb.setMouseEnabled(x=False, y=True)

    def on_rect_zoom(self):
        for pw in self.plots:
            vb = pw.getViewBox()
            vb.setMouseMode(pg.ViewBox.RectMode)
            vb.setMouseEnabled(x=True, y=True)

    def on_refresh(self):
        print("[REFRESH] Clicked")
        selected_start = self._selected_start_dt()

        exch_text = (self.exchange_combo.currentText() or "").strip()
        exchange_filter = None if (not exch_text or exch_text.lower() == "all") else exch_text

        sym_text = (self.symbol_combo.currentText() or "").strip()
        symbol_filter = None if (not sym_text or sym_text.lower() == "all") else sym_text

        try:
            if self.df is not None and not self.df.empty and "ts" in self.df.columns:
                current_max = self.df["ts"].max()
            else:
                current_max = None
        except Exception:
            current_max = None

        try:
            db_max = get_latest_ts_in_db(selected_start, exchange_filter, symbol_filter)
        except Exception as e:
            traceback.print_exc()
            QtWidgets.QMessageBox.warning(self, "Refresh", f"DB error:\n{e}")
            return

        if db_max is None:
            QtWidgets.QMessageBox.information(self, "Refresh", "No data in DB.")
            return

        if current_max is not None:
            current_cmp = pd.to_datetime(current_max).to_pydatetime().replace(tzinfo=None)
        else:
            current_cmp = None
        db_cmp = pd.to_datetime(db_max).to_pydatetime().replace(tzinfo=None)

        if (
            current_cmp is not None
            and current_cmp >= db_cmp
            and self.current_start_dt is not None
            and self.current_start_dt == selected_start
            and self.current_exchange == exchange_filter
            and self.current_symbol == symbol_filter
        ):
            QtWidgets.QMessageBox.information(self, "Refresh", "Up to date.")
            return

        try:
            new_df = load_stats(start_dt=selected_start, exchange=exchange_filter, symbol=symbol_filter)
        except Exception as e:
            traceback.print_exc()
            QtWidgets.QMessageBox.warning(self, "Refresh", f"Reload failed:\n{e}")
            return

        if new_df.empty:
            QtWidgets.QMessageBox.information(self, "Refresh", "No data after reload.")
            return

        self.df = new_df
        self.current_start_dt = selected_start
        self.current_exchange = exchange_filter
        self.current_symbol = symbol_filter

        # clear plots and caches
        for pw in self.plots:
            pw.clear()

        self.segment_data = [dict() for _ in self.plots]
        self.segment_ts_sorted = [[] for _ in self.plots]
        self.color_map.clear()
        self.used_instruments.clear()
        self.plot_bars_by_instrument = [dict() for _ in self.plots]
        self.instrument_rank1_max_height.clear()
        self.legend_items.clear()
        self.legend_widget.clear()
        self.visible_instruments.clear()
        self.separator_items = [[] for _ in self.plots]

        self._populate_filter_combos(new_df)
        self._populate_plots()
        self._build_legend()


# ============= MAIN =============

def main():
    print("[MAIN] arb_stats_pyqt starting")

    default_start = _utcnow_naive() - timedelta(hours=2)

    app = QtWidgets.QApplication([])
    empty_df = pd.DataFrame()
    win = StatsWindow(empty_df, default_start)
    win.show()
    print("[MAIN] StatsWindow shown, entering Qt event loop")
    app.exec()
    print("[MAIN] Qt event loop exited")


if __name__ == "__main__":
    main()
