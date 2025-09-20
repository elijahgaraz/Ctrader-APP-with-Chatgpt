from abc import ABC, abstractmethod
from typing import Any, Dict, TYPE_CHECKING, Union
import pandas as pd
from datetime import time, datetime
from zoneinfo import ZoneInfo  # Python 3.9+

from indicators import calculate_ema, calculate_atr, calculate_rsi, calculate_adx

if TYPE_CHECKING:
    from trading import Trader


class Strategy(ABC):
    """Abstract base class for trading strategies."""
    NAME: str = "Base Strategy"

    @abstractmethod
    def decide(self, symbol: str, data: Dict[str, Any], trader: "Trader") -> Dict[str, Any]:
        return {
            'action': 'hold',
            'comment': f'{self.NAME} not implemented',
            'sl_offset': None,
            'tp_offset': None
        }

    @abstractmethod
    def get_required_bars(self) -> Dict[str, int]:
        """Returns a dict of {'timeframe_str': count} required by the strategy."""
        return {}


class SafeStrategy(Strategy):
    """
    Enhanced Safe (Low-Risk) Trend-Following Scalper with:
      - Volatility regime filters
      - Trend buffer zone around EMA
      - Session time filter (robust to tz/epoch)
      - Volume spike filter
      - Trailing stop activation
    """
    NAME = "Safe (Low-Risk) Trend-Following Scalper"

    def __init__(
        self,
        settings,
        ema_period: int = 50,
        atr_period: int = 14,
        stop_mult: float = 1.0,
        target_mult: float = 0.5,
        buffer_mult: float = 0.05,  # changed from 0.2 to allow more trades; adjust as needed
        volume_mult: float = 1.5,
        session_tz: str = "Europe/London",
    ):
        # Trend & volatility settings
        self.settings = settings
        self.ema_period = ema_period
        self.atr_period = atr_period
        self.stop_mult = stop_mult
        self.target_mult = target_mult
        self.buffer_mult = buffer_mult
        self.volume_mult = volume_mult

        # Trading session window
        start_h = self.settings.general.trading_start_hour
        end_h = self.settings.general.trading_end_hour
        self.session_start = time(start_h, 0)
        self.session_end = time(end_h, 0) if end_h < 24 else time(23, 59, 59)
        self.session_zone = ZoneInfo(session_tz)

        # Trailing stop state
        self.trailing_activated = False

    def get_required_bars(self) -> Dict[str, int]:
        return {'1m': self.settings.general.min_bars_for_trading}

    # ---------- Robust timestamp helpers ----------
    def _to_session_dt(self, ts: Union[pd.Timestamp, int, float, datetime]) -> datetime:
        """
        Convert ts to timezone-aware datetime in the session timezone.
        Handles:
          - pandas Timestamp (naive or tz-aware)
          - epoch seconds or milliseconds (int/float)
          - datetime (naive or tz-aware)
        Assumes UTC when the source is naive (adjust if your feed differs).
        """
        if isinstance(ts, pd.Timestamp):
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            dt_utc = ts.to_pydatetime().astimezone(ZoneInfo("UTC"))
        elif isinstance(ts, (int, float)):
            # Heuristic: > 1e12 → milliseconds
            seconds = ts / 1000.0 if ts > 1_000_000_000_000 else ts
            dt_utc = datetime.fromtimestamp(seconds, tz=ZoneInfo("UTC"))
        elif isinstance(ts, datetime):
            if ts.tzinfo is None:
                dt_utc = ts.replace(tzinfo=ZoneInfo("UTC"))
            else:
                dt_utc = ts.astimezone(ZoneInfo("UTC"))
        else:
            raise TypeError(f"Unsupported timestamp type: {type(ts)}")

        return dt_utc.astimezone(self.session_zone)

    def _extract_latest_ts(self, df: pd.DataFrame):
        """
        Return the most recent timestamp from df as one of:
        - pandas.Timestamp (tz-aware or naive)
        - int/float epoch seconds or milliseconds
        Tries DatetimeIndex first; otherwise checks common time columns.
        """
        # 1) If index is already a DatetimeIndex, use it
        if isinstance(df.index, pd.DatetimeIndex):
            return df.index[-1]

        # 2) Otherwise, look for a time-like column
        candidate_cols = [
            "time", "timestamp", "datetime", "date",
            "open_time", "close_time"
        ]
        for col in candidate_cols:
            if col in df.columns:
                s = df[col].iloc[-1]

                # If already a pandas Timestamp
                if isinstance(s, pd.Timestamp):
                    return s

                # If string/ISO8601 parseable
                if isinstance(s, str):
                    parsed = pd.to_datetime(s, errors="coerce", utc=True)
                    if pd.notna(parsed):
                        return parsed

                # If numeric, treat as epoch (auto-detect ms vs s)
                if isinstance(s, (int, float)):
                    return s

        # 3) Fallback: detect any datetime-like column by dtype
        dt_like_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
        if dt_like_cols:
            return df[dt_like_cols[0]].iloc[-1]

        # 4) Nothing suitable found → raise clear error
        raise ValueError(
            "No datetime information found. Provide a DatetimeIndex or a time column "
            "(e.g., 'time', 'timestamp', 'datetime')."
        )

    def in_session(self, timestamp: Union[pd.Timestamp, int, float, datetime]) -> bool:
        local_dt = self._to_session_dt(timestamp)
        t = local_dt.time()

        # If your session crosses midnight, handle it:
        if self.session_start <= self.session_end:
            return self.session_start <= t <= self.session_end
        else:
            # e.g., 22:00–06:00
            return (t >= self.session_start) or (t <= self.session_end)

    def _hold(self, reason: str) -> Dict[str, Any]:
        return {
            'action': 'hold',
            'comment': f"{self.NAME}: {reason}",
            'sl_offset': None,
            'tp_offset': None
        }

    def decide(self, symbol: str, data: Dict[str, Any], trader: "Trader") -> Dict[str, Any]:
        df: pd.DataFrame = data.get('ohlc_1m')
        print("DECIDE() called - OHLC shape:", df.shape if df is not None else "None")
        if df is None or len(df) < self.settings.general.min_bars_for_trading:
            print("Returning: insufficient data")
            return self._hold("insufficient data")

        # Session filter with robust timestamp handling
        print("DEBUG - df.index:", df.index)
        print("DEBUG - df.index[-1]:", df.index[-1] if len(df.index) else None, type(df.index[-1]) if len(df.index) else None)

        now_raw = self._extract_latest_ts(df)
        now_local = self._to_session_dt(now_raw)
        print(f"DEBUG - now_raw={now_raw!r} type={type(now_raw)}, now_local={now_local} [{self.session_zone}]")

        if not self.in_session(now_raw):
            return self._hold("outside trading session")

        close = df['close']
        vol = df.get('volume', pd.Series(dtype=float))

        # Indicators
        ema = calculate_ema(df, self.ema_period).iloc[-1]
        atr = calculate_atr(df, self.atr_period).iloc[-1]
        price = close.iloc[-1]
        avg_vol = None if vol.empty else vol.rolling(self.atr_period).mean().iloc[-1]

        # Buffer zone filter
        buffer = atr * self.buffer_mult
        if abs(price - ema) < buffer:
            return self._hold("within buffer zone")

        # Determine trade direction
        if price > ema:
            action = 'buy'
            comment = f"price {price:.5f} above EMA{self.ema_period} + buffer"
        else:
            action = 'sell'
            comment = f"price {price:.5f} below EMA{self.ema_period} - buffer"

        # Base stops
        sl = atr * self.stop_mult
        tp = atr * self.target_mult

        # Convert offsets from price distance to pips (1 pip = 0.0001 for most pairs)
        pip_factor = 10000  # Use 100 for JPY pairs like USDJPY
        sl_pips = sl * pip_factor
        tp_pips = tp * pip_factor

        # Trailing stop logic
        if not self.trailing_activated and (
            (action == 'buy' and price > ema + 2 * buffer) or
            (action == 'sell' and price < ema - 2 * buffer)
        ):
            self.trailing_activated = True
            comment += "; trailing stop activated"

        if self.trailing_activated:
            breakeven_offset = atr * 0.1
            prev_close = close.iloc[-2]
            if action == 'buy':
                sl = min(sl, price - (prev_close + breakeven_offset))
            else:
                sl = min(sl, (prev_close - breakeven_offset) - price)

        # --- AI Overseer Integration ---
        if trader.settings.ai.use_ai_overseer and action in ('buy', 'sell'):
            # 1) Calculate all indicators for the AI payload
            intent = 'long' if action == 'buy' else 'short'
            fast_ema = calculate_ema(df, 9).iloc[-1]
            slow_ema = calculate_ema(df, 21).iloc[-1]
            rsi = calculate_rsi(df, 14).iloc[-1]
            adx_df = calculate_adx(df, 14)
            adx = adx_df[f'ADX_14'].iloc[-1] if not adx_df.empty else 0

            # 2) Construct payload
            features = {
                "price_bid": price,
                "ema_fast": fast_ema,
                "ema_slow": slow_ema,
                "rsi": rsi,
                "adx": adx,
                "atr": atr,
                "spread_pips": 0
            }
            bot_proposal = {"side": intent, "sl_pips": sl_pips, "tp_pips": tp_pips}

            # 3) Get AI advice
            ai_advice = trader.get_ai_advice(symbol, intent, features, bot_proposal)

            # 4) Act on AI advice
            if ai_advice:
                ai_action_map = {'long': 'buy', 'short': 'sell'}
                if ai_advice.confidence < trader.settings.ai.advisor_min_confidence:
                    return self._hold(
                        f"AI confidence {ai_advice.confidence:.2%} below threshold "
                        f"{trader.settings.ai.advisor_min_confidence:.2%}. AI Reason: {ai_advice.reason}"
                    )

                if ai_action_map.get(ai_advice.action) != action:
                    return self._hold(
                        f"AI action '{ai_advice.action}' contradicts strategy '{action}'. "
                        f"AI Reason: {ai_advice.reason}"
                    )

                # If advice is good, augment the comment
                comment += f" | AI Confirmed (Conf: {ai_advice.confidence:.2%}, Reason: {ai_advice.reason})"
            else:
                # If AI fails to provide advice, revert to holding for safety
                return self._hold("AI advisor failed to provide a valid response.")

        return {
            'action': action,
            'comment': f"{self.NAME}: {comment}",
            'sl_offset': sl_pips,
            'tp_offset': tp_pips,
            'risk_percentage': self.settings.general.risk_percentage
        }


class ModerateStrategy(Strategy):
    NAME = "Moderate Trend-Following Scalper"

    def __init__(self, settings):
        self.settings = settings
        self.ema_period = 20
        self.atr_period = 14
        self.stop_multiplier = 1.5
        self.target_multiplier = 1.0

    def get_required_bars(self) -> Dict[str, int]:
        return {'1m': self.settings.general.min_bars_for_trading}

    def decide(self, symbol: str, data: Dict[str, Any], trader: "Trader") -> Dict[str, Any]:
        df: pd.DataFrame = data.get('ohlc_1m')
        if df is None or len(df) < self.settings.general.min_bars_for_trading:
            return {
                'action': 'hold',
                'comment': f'{self.NAME}: insufficient data',
                'sl_offset': None,
                'tp_offset': None
            }

        ema = calculate_ema(df['close'], self.ema_period).iloc[-1]
        atr = calculate_atr(df, self.atr_period).iloc[-1]
        price = df['close'].iloc[-1]

        if price > ema:
            action = 'buy'
            comment = f'{self.NAME}: bullish trend detected'
        elif price < ema:
            action = 'sell'
            comment = f'{self.NAME}: bearish trend detected'
        else:
            return {
                'action': 'hold',
                'comment': f'{self.NAME}: no clear trend',
                'sl_offset': None,
                'tp_offset': None
            }

        sl_offset = atr * self.stop_multiplier
        tp_offset = atr * self.target_multiplier
        return {'action': action, 'comment': comment, 'sl_offset': sl_offset, 'tp_offset': tp_offset}


class AggressiveStrategy(Strategy):
    NAME = "Aggressive Trend-Following Scalper"

    def __init__(self, settings):
        self.settings = settings
        self.ema_period = 10
        self.atr_period = 7
        self.stop_multiplier = 2.0
        self.target_multiplier = 1.5

    def get_required_bars(self) -> Dict[str, int]:
        return {'1m': self.settings.general.min_bars_for_trading}

    def decide(self, symbol: str, data: Dict[str, Any], trader: "Trader") -> Dict[str, Any]:
        df: pd.DataFrame = data.get('ohlc_1m')
        if df is None or len(df) < self.settings.general.min_bars_for_trading:
            return {
                'action': 'hold',
                'comment': f'{self.NAME}: insufficient data',
                'sl_offset': None,
                'tp_offset': None
            }

        ema = calculate_ema(df['close'], self.ema_period).iloc[-1]
        atr = calculate_atr(df, self.atr_period).iloc[-1]
        price = df['close'].iloc[-1]

        if price > ema:
            action = 'buy'
            comment = f'{self.NAME}: going long aggressively'
        elif price < ema:
            action = 'sell'
            comment = f'{self.NAME}: going short aggressively'
        else:
            return {
                'action': 'hold',
                'comment': f'{self.NAME}: awaiting breakout',
                'sl_offset': None,
                'tp_offset': None
            }

        sl_offset = atr * self.stop_multiplier
        tp_offset = atr * self.target_multiplier
        return {'action': action, 'comment': comment, 'sl_offset': sl_offset, 'tp_offset': tp_offset}


class MomentumStrategy(Strategy):
    NAME = "Momentum Fade Scalper"

    def __init__(self, settings):
        self.settings = settings
        self.ema_period = 20
        self.atr_period = 14
        self.fade_threshold = 1.5  # ATR multiples
        self.stop_multiplier = 1.0
        self.target_multiplier = 1.5

    def get_required_bars(self) -> Dict[str, int]:
        return {'1m': self.settings.general.min_bars_for_trading}

    def decide(self, symbol: str, data: Dict[str, Any], trader: "Trader") -> Dict[str, Any]:
        df: pd.DataFrame = data.get('ohlc_1m')
        if df is None or len(df) < self.settings.general.min_bars_for_trading:
            return {
                'action': 'hold',
                'comment': f'{self.NAME}: insufficient data',
                'sl_offset': None,
                'tp_offset': None
            }

        ema = calculate_ema(df['close'], self.ema_period).iloc[-1]
        atr = calculate_atr(df, self.atr_period).iloc[-1]
        price = df['close'].iloc[-1]
        diff = price - ema

        if diff > atr * self.fade_threshold:
            action = 'sell'
            comment = f'{self.NAME}: fading overextension'
        elif diff < -atr * self.fade_threshold:
            action = 'buy'
            comment = f'{self.NAME}: fading downside spike'
        else:
            return {
                'action': 'hold',
                'comment': f'{self.NAME}: no fade opportunity',
                'sl_offset': None,
                'tp_offset': None
            }

        sl_offset = atr * self.stop_multiplier
        tp_offset = atr * self.target_multiplier
        return {'action': action, 'comment': comment, 'sl_offset': sl_offset, 'tp_offset': tp_offset}


class MeanReversionStrategy(Strategy):
    NAME = "Mean-Reversion Scalper"

    def __init__(self, settings):
        self.settings = settings
        self.ema_period = 20
        self.atr_period = 14
        self.band_multiplier = 2.0  # ATR multiples
        self.stop_multiplier = 1.0
        self.target_multiplier = 2.0

    def get_required_bars(self) -> Dict[str, int]:
        return {'1m': self.settings.general.min_bars_for_trading}

    def decide(self, symbol: str, data: Dict[str, Any], trader: "Trader") -> Dict[str, Any]:
        df: pd.DataFrame = data.get('ohlc_1m')
        if df is None or len(df) < self.settings.general.min_bars_for_trading:
            return {
                'action': 'hold',
                'comment': f'{self.NAME}: insufficient data',
                'sl_offset': None,
                'tp_offset': None
            }

        ema = calculate_ema(df['close'], self.ema_period).iloc[-1]
        atr = calculate_atr(df, self.atr_period).iloc[-1]
        price = df['close'].iloc[-1]
        upper = ema + atr * self.band_multiplier
        lower = ema - atr * self.band_multiplier

        if price > upper:
            action = 'sell'
            comment = f'{self.NAME}: price above upper band'
        elif price < lower:
            action = 'buy'
            comment = f'{self.NAME}: price below lower band'
        else:
            return {
                'action': 'hold',
                'comment': f'{self.NAME}: within bands',
                'sl_offset': None,
                'tp_offset': None
            }

        sl_offset = atr * self.stop_multiplier
        tp_offset = atr * self.target_multiplier
        return {'action': action, 'comment': comment, 'sl_offset': sl_offset, 'tp_offset': tp_offset}
