"""
변동성 돌파 전략 (Larry Williams) + 워뇨띠 + 웅크웅크 필터 통합

신호 체계
─────────────────────────────────────────────────────────────────────
 signal │ 의미
────────┼────────────────────────────────────────────────────────────
   1    │ 롱 진입 (상방 돌파 + 모든 필터 통과)
  -1    │ 롱 청산
   2    │ 숏 진입 (추세 팔로잉) — OKX 전용
  -2    │ 숏 청산
─────────────────────────────────────────────────────────────────────

롱 필터 레이어 (AND 조건)
────────────────────────────────────────
 1. MA200 추세 필터       (기존)
 2. 거래량 급증 필터      (워뇨띠)
 3. 다중 EMA 정렬 필터   (웅크웅크 기법 2)
 4. Volume Profile 필터  (웅크웅크 기법 1 근사)
 5. 피보나치 근접 가중치  (웅크웅크 기법 3 — 선택적)
────────────────────────────────────────

숏 조건 — 추세 팔로잉 (B안)
────────────────────────────────────────
 1. close < MA200          (하락 추세)
 2. ema_20 < ema_55        (EMA 하향 정렬)
 3. 연속 N봉 하락           (short_consec일 연속 close 하락)
────────────────────────────────────────
"""
import numpy as np
import pandas as pd
from .base_strategy import BaseStrategy
from .indicators import (
    calc_multi_ema,
    ema_aligned_long,
    ema_aligned_short,
    calc_volume_profile,
    calc_fibonacci,
    volume_surge,
    near_fib_level,
    calc_atr,
    calc_supertrend,
    calc_macd,
    calc_bollinger_bands,
    detect_rsi_divergence,
    detect_obv_divergence,
    detect_hammer,
    detect_shooting_star,
    calc_ema_stack,
)


class VolatilityBreakoutStrategy(BaseStrategy):
    """
    Parameters
    ----------
    k                   : 변동폭 비율 (기본 0.5)
    ma_period           : 추세 필터 이동평균 기간 (기본 200)
    use_short           : 숏 전략 사용 여부 (OKX 전용)
    volume_lookback     : 거래량 필터 평균 기간 (기본 20)
    volume_multiplier   : 거래량 급증 배수 (기본 1.5)
    vp_lookback         : Volume Profile 기간 (기본 20)
    vp_bins             : Volume Profile 가격 구간 수 (기본 50)
    fib_lookback        : 피보나치 스윙 탐색 기간 (기본 50)
    fib_bonus           : True이면 피보나치 근접 시 신호 기록 (로그용)
    short_consec        : 숏 진입에 필요한 연속 하락 봉 수 (기본 2)
    use_supertrend      : Supertrend 추세 필터 활성화 (OKX 선물 권장)
    supertrend_period   : Supertrend ATR 기간 (기본 7)
    supertrend_mult     : Supertrend ATR 배수 (기본 3.0)
    use_macd_filter     : MACD 방향 필터 활성화
    use_atr_sl          : ATR 기반 동적 SL/TP 컬럼 출력
    atr_period          : ATR 계산 기간 (기본 14)
    atr_sl_mult         : SL = close ± ATR × atr_sl_mult
    atr_tp_mult         : TP = close ± ATR × atr_tp_mult
    use_rsi_div         : RSI 다이버전스 → confidence +1
    use_bb_squeeze      : BB squeeze → confidence +1
    """

    def __init__(
        self,
        k: float = 0.5,
        ma_period: int = 200,
        use_short: bool = False,
        volume_lookback: int = 20,
        volume_multiplier: float = 1.5,
        vp_lookback: int = 20,
        vp_bins: int = 50,
        fib_lookback: int = 50,
        fib_bonus: bool = True,
        short_consec: int = 2,
        use_supertrend: bool = False,
        supertrend_period: int = 7,
        supertrend_mult: float = 3.0,
        use_macd_filter: bool = False,
        use_atr_sl: bool = False,
        atr_period: int = 14,
        atr_sl_mult: float = 1.5,
        atr_tp_mult: float = 3.0,
        use_rsi_div: bool = False,
        use_bb_squeeze: bool = False,
    ):
        name_parts = [f"VB+MA{ma_period}"]
        name_parts.append("Vol")
        name_parts.append("EMA")
        name_parts.append("VP")
        if use_supertrend:
            name_parts.append("ST")
        if use_macd_filter:
            name_parts.append("MACD")
        if use_short:
            name_parts.append("L/S")

        super().__init__(
            name="+".join(name_parts),
            params={
                "k": k,
                "ma_period": ma_period,
                "use_short": use_short,
                "volume_multiplier": volume_multiplier,
                "use_supertrend": use_supertrend,
                "use_macd_filter": use_macd_filter,
                "use_atr_sl": use_atr_sl,
            },
        )
        self.k = k
        self.ma_period = ma_period
        self.use_short = use_short
        self.volume_lookback = volume_lookback
        self.volume_multiplier = volume_multiplier
        self.vp_lookback = vp_lookback
        self.vp_bins = vp_bins
        self.fib_lookback = fib_lookback
        self.fib_bonus = fib_bonus
        self.short_consec = short_consec
        self.use_supertrend = use_supertrend
        self.supertrend_period = supertrend_period
        self.supertrend_mult = supertrend_mult
        self.use_macd_filter = use_macd_filter
        self.use_atr_sl = use_atr_sl
        self.atr_period = atr_period
        self.atr_sl_mult = atr_sl_mult
        self.atr_tp_mult = atr_tp_mult
        self.use_rsi_div = use_rsi_div
        self.use_bb_squeeze = use_bb_squeeze

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # ── 지표 계산 ──────────────────────────────────────────────────
        # MA200
        df["ma200"] = df["close"].rolling(self.ma_period).mean()

        # 다중 EMA (웅크웅크 기법 2)
        df = calc_multi_ema(df, periods=[20, 55, 100, 200])

        # 변동성 돌파 목표가
        df["prev_range"]    = df["high"].shift(1) - df["low"].shift(1)
        df["target_long"]   = df["open"] + df["prev_range"] * self.k
        df["target_short"]  = df["open"] - df["prev_range"] * self.k

        # 거래량 급증 필터 (워뇨띠)
        df["vol_surge"] = volume_surge(df, self.volume_lookback, self.volume_multiplier)

        # Volume Profile (웅크웅크 기법 1)
        vp_df = calc_volume_profile(df, self.vp_lookback, self.vp_bins)
        df["vp_poc"] = vp_df["poc"]
        df["vp_vah"] = vp_df["vah"]
        df["vp_val"] = vp_df["val"]

        # 피보나치 (웅크웅크 기법 3)
        fib_df = calc_fibonacci(df, self.fib_lookback)
        for col in fib_df.columns:
            df[col] = fib_df[col]

        # ── 신규 지표 계산 ─────────────────────────────────────────────

        # ATR (Supertrend 및 동적 SL/TP에 공통 사용)
        if self.use_supertrend or self.use_atr_sl:
            df = calc_atr(df, self.atr_period)

        # Supertrend
        if self.use_supertrend:
            df = calc_supertrend(df, self.supertrend_period, self.supertrend_mult)

        # MACD
        if self.use_macd_filter:
            df = calc_macd(df)

        # Bollinger Bands (BB Squeeze)
        if self.use_bb_squeeze:
            df = calc_bollinger_bands(df)

        # RSI Divergence
        if self.use_rsi_div:
            df = detect_rsi_divergence(df)

        # ATR 기반 동적 SL/TP 컬럼 (롱/숏 기준)
        if self.use_atr_sl and "atr" in df.columns:
            df["atr_sl_long"]  = df["close"] - df["atr"] * self.atr_sl_mult
            df["atr_tp_long"]  = df["close"] + df["atr"] * self.atr_tp_mult
            df["atr_sl_short"] = df["close"] + df["atr"] * self.atr_sl_mult
            df["atr_tp_short"] = df["close"] - df["atr"] * self.atr_tp_mult

        # ── 필터 조건 ──────────────────────────────────────────────────
        valid = df["prev_range"].notna() & (df["prev_range"] > 0)

        # 추세 필터
        uptrend   = df["close"] > df["ma200"]
        downtrend = df["close"] < df["ma200"]

        # EMA 정렬 (웅크웅크 기법 2)
        ema_long  = ema_aligned_long(df)
        ema_short = ema_aligned_short(df)

        # 거래량 급증
        vol_ok = df["vol_surge"]

        # Volume Profile 지지/저항
        # 롱: VAL 위에서 매수 (VAL 아래는 약세 구간)
        # 숏: VAH 아래에서 매도 (VAH 위는 강세 구간)
        vp_long_ok  = df["close"] >= df["vp_val"].fillna(0)
        vp_short_ok = df["close"] <= df["vp_vah"].fillna(float("inf"))

        # 변동성 돌파
        long_breakout  = (df["high"] >= df["target_long"])  & (df["open"] < df["target_long"])
        short_breakout = (df["low"]  <= df["target_short"]) & (df["open"] > df["target_short"])

        # Supertrend 방향 필터
        st_long_ok  = df["supertrend_dir"] == 1  if self.use_supertrend else pd.Series(True, index=df.index)
        st_short_ok = df["supertrend_dir"] == -1 if self.use_supertrend else pd.Series(True, index=df.index)

        # MACD 방향 필터 (히스토그램 방향)
        if self.use_macd_filter and "macd_hist" in df.columns:
            macd_long_ok  = df["macd_hist"] > 0
            macd_short_ok = df["macd_hist"] < 0
        else:
            macd_long_ok  = pd.Series(True, index=df.index)
            macd_short_ok = pd.Series(True, index=df.index)

        # ── 신호 생성 ──────────────────────────────────────────────────
        df["signal"] = 0

        long_cond = (
            valid & uptrend & ema_long & vol_ok & vp_long_ok & long_breakout
            & st_long_ok & macd_long_ok
        )
        df.loc[long_cond, "signal"] = 1

        if self.use_short:
            if self.use_supertrend and "supertrend_dir" in df.columns:
                # Supertrend 하락 방향 + 당일 음봉 → consec_down 대체
                # consec_down(연속 N봉)보다 빠르게 반응, ATR 기반으로 변동성 자동 적응
                short_timing = (
                    (df["supertrend_dir"] == -1)
                    & (df["close"] < df["close"].shift(1))
                )
                short_cond = valid & downtrend & ema_short & short_timing & macd_short_ok
            else:
                # Supertrend 미사용 시 기존 연속 하락 방식
                consec_down = pd.Series(True, index=df.index)
                for lag in range(self.short_consec):
                    consec_down &= df["close"].shift(lag) < df["close"].shift(lag + 1)
                short_cond = valid & downtrend & ema_short & consec_down & macd_short_ok

            df.loc[short_cond, "signal"] = 2

        # 피보나치 근접 여부 기록 (로그/디버깅용 — 신호 필터는 아님)
        if self.fib_bonus:
            fib_cols = [c for c in df.columns if c.startswith("fib_")]
            df["fib_near"] = df.apply(
                lambda row: near_fib_level(
                    row["close"],
                    {c: row[c] for c in fib_cols},
                ),
                axis=1,
            )

        # ── 확신도 점수 (1~5) → 동적 레버리지 기반 ────────────────────
        # 신호가 발생한 행에만 의미 있음 (나머지는 1로 초기화)
        confidence = pd.Series(1, index=df.index)
        signal_mask = df["signal"].isin([1, 2])

        # +1 : 거래량이 기준배수의 2배 이상 (롱 전용 — 강한 급등 신호)
        avg_vol = df["volume"].rolling(self.volume_lookback).mean()
        confidence += (
            signal_mask & (df["signal"] == 1) &
            (df["volume"] > avg_vol * self.volume_multiplier * 2)
        ).astype(int)

        # +1 : 피보나치 레벨 근접 (웅크웅크)
        if "fib_near" in df.columns:
            confidence += df["fib_near"].astype(int)

        # +1 : EMA 단기-중기 간격이 중기 대비 1% 이상 (정렬 강도)
        if "ema_20" in df.columns and "ema_55" in df.columns:
            ema_gap = (df["ema_20"] - df["ema_55"]).abs() / df["ema_55"]
            confidence += (ema_gap > 0.01).astype(int)

        # +1 : MA200 대비 거리 5% 이상 (추세 강도)
        if "ma200" in df.columns:
            ma_dist = (df["close"] - df["ma200"]).abs() / df["ma200"]
            confidence += (ma_dist > 0.05).astype(int)

        # +1 : 숏 전용 — 연속 하락이 short_consec+1봉 이상 (더 강한 추세)
        if self.use_short:
            extra_down = pd.Series(True, index=df.index)
            for lag in range(self.short_consec + 1):
                extra_down &= df["close"].shift(lag) < df["close"].shift(lag + 1)
            confidence += (
                signal_mask & (df["signal"] == 2) & extra_down
            ).astype(int)

        # +1 : RSI 상승 다이버전스 → 롱 신뢰도 보강
        if self.use_rsi_div and "rsi_bull_div" in df.columns:
            confidence += (
                signal_mask & (df["signal"] == 1) & df["rsi_bull_div"]
            ).astype(int)

        # +1 : RSI 하락 다이버전스 → 숏 신뢰도 보강
        if self.use_rsi_div and "rsi_bear_div" in df.columns:
            confidence += (
                signal_mask & (df["signal"] == 2) & df["rsi_bear_div"]
            ).astype(int)

        # +1 : BB Squeeze (변동성 수축) — 대형 움직임 임박
        if self.use_bb_squeeze and "bb_width" in df.columns:
            bb_avg = df["bb_width"].rolling(20).mean()
            squeeze = df["bb_width"] < bb_avg * 0.5
            confidence += (signal_mask & squeeze).astype(int)

        # ── EmperorBTC 기법 확신도 보정 ────────────────────────────────
        # 각 행(신호 발생 시점)에 대해 개별 계산 — 슬라이싱으로 현재 봉까지의 df 전달
        for idx in df.index[signal_mask]:
            loc = df.index.get_loc(idx)
            sub = df.iloc[: loc + 1]  # 현재 봉 포함 이전 데이터
            if len(sub) < 15:
                continue

            sig = df.at[idx, "signal"]
            c = confidence.at[idx]

            # Volume-Price 방향성 (EmperorBTC Volume 4 Scenario)
            last_close = sub["close"].iloc[-1]
            prev_close = sub["close"].iloc[-2]
            last_vol   = sub["volume"].iloc[-1]
            avg_vol14  = sub["volume"].rolling(14).mean().iloc[-1]
            price_up   = last_close > prev_close
            vol_up     = last_vol > avg_vol14

            if sig == 1 and price_up and not vol_up:
                # 가격↑ + 거래량↓ = 돌파 신뢰도 낮음
                c = max(1, c - 1)
            elif sig == 2 and not price_up and not vol_up:
                # 가격↓ + 거래량↓ = 정상 조정, 숏 약화
                c = max(1, c - 1)

            # OBV Divergence (EmperorBTC: 추세 확인)
            obv_div = detect_obv_divergence(sub)
            if sig == 1 and obv_div == "bearish":
                c = max(1, c - 1)
            elif sig == 2 and obv_div == "bullish":
                c = max(1, c - 1)

            # Hammer (EmperorBTC: 하락추세 바닥 반전 신호)
            if sig == 1 and detect_hammer(sub):
                c = min(5, c + 1)

            # Shooting Star (EmperorBTC: 상승추세 천장 경고)
            if sig == 1 and detect_shooting_star(sub):
                c = max(1, c - 1)

            # EMA Stack 13/21 (EmperorBTC: 단기 추세 정렬)
            ema_stack = calc_ema_stack(sub)
            if sig == 1 and ema_stack["bullish"]:
                c = min(5, c + 1)
            elif sig == 2 and ema_stack["bearish"]:
                c = min(5, c + 1)
            elif sig == 1 and ema_stack["bearish"]:
                c = max(1, c - 1)

            confidence.at[idx] = c

        df["confidence"] = confidence.clip(1, 5)

        # ── 포지션 / 수익률 ────────────────────────────────────────────
        df["position"] = df["signal"].apply(lambda s: 1 if s in (1, 2) else 0)

        df["entry_price"] = np.where(
            df["signal"] == 1, df["target_long"],
            np.where(df["signal"] == 2, df["target_short"], np.nan),
        )
        df["exit_price"] = df["open"].shift(-1)

        long_ret  = (df["exit_price"] - df["target_long"])  / df["target_long"]
        short_ret = (df["target_short"] - df["exit_price"]) / df["target_short"]
        df["strategy_return"] = np.where(
            df["signal"] == 1, long_ret,
            np.where(df["signal"] == 2, short_ret, 0.0),
        )

        return df.dropna(subset=["prev_range"])
