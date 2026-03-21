"""
텔레그램 알림 모듈

기능:
  - 매수/매도 체결 알림
  - 손절/익절 알림
  - 일일 수익률 리포트
  - 에러 알림

사용법:
  1. @BotFather 에서 봇 생성 → 토큰 발급
  2. 봇과 대화 후 /start → chat_id 확인
  3. config/.env 에 TELEGRAM_TOKEN, TELEGRAM_CHAT_ID 입력
"""
import os
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../config/.env"))

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """
    Parameters
    ----------
    enabled : False이면 모든 알림 무시 (테스트 시 편리)
    """

    def __init__(self, enabled: bool = None):
        self.token = os.getenv("TELEGRAM_TOKEN", "")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

        if enabled is None:
            self.enabled = bool(self.token and self.chat_id)
        else:
            self.enabled = enabled

        if self.enabled:
            logger.info("[Telegram] 알림 활성화")
        else:
            logger.info("[Telegram] 알림 비활성화 (토큰 미설정)")

    def send(self, message: str) -> bool:
        """텔레그램 메시지 전송"""
        if not self.enabled:
            logger.debug(f"[Telegram 비활성화] {message}")
            return False

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "HTML",
        }
        try:
            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"텔레그램 전송 실패: {e}")
            return False

    # ── 편의 메서드 ────────────────────────────────────────

    def notify_buy(self, market: str, price: float, amount: float, currency: str = "KRW"):
        is_usdt = currency.upper() == "USDT"
        if is_usdt:
            price_str  = f"${price:,.4f} USDT"
            amount_str = f"${amount:,.2f} USDT"
        else:
            price_str  = f"{price:,.0f}원"
            amount_str = f"{amount:,.0f}원"
        msg = (
            f"🟢 <b>진입 체결</b>\n"
            f"종목: {market}\n"
            f"가격: {price_str}\n"
            f"<b>투자금액: {amount_str}</b>\n"
            f"시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        self.send(msg)

    def notify_sell(self, market: str, price: float, volume: float, pnl_pct: float):
        emoji = "📈" if pnl_pct >= 0 else "📉"
        msg = (
            f"{emoji} <b>매도 체결</b>\n"
            f"종목: {market}\n"
            f"가격: {price:,.0f}원\n"
            f"수량: {volume} 코인\n"
            f"수익률: {pnl_pct:+.2f}%\n"
            f"시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        self.send(msg)

    def notify_stop_loss(self, market: str, entry: float, current: float):
        loss = (current - entry) / entry * 100
        msg = (
            f"🛑 <b>손절 실행</b>\n"
            f"종목: {market}\n"
            f"진입가: {entry:,.0f}원\n"
            f"청산가: {current:,.0f}원\n"
            f"손실: {loss:.2f}%"
        )
        self.send(msg)

    def notify_take_profit(self, market: str, entry: float, current: float):
        gain = (current - entry) / entry * 100
        msg = (
            f"✅ <b>익절 실행</b>\n"
            f"종목: {market}\n"
            f"진입가: {entry:,.0f}원\n"
            f"청산가: {current:,.0f}원\n"
            f"수익: +{gain:.2f}%"
        )
        self.send(msg)

    def notify_daily_report(self, capital: float, daily_return_pct: float, trades_today: int):
        emoji = "📊"
        msg = (
            f"{emoji} <b>일일 리포트</b>  {datetime.now().strftime('%Y-%m-%d')}\n"
            f"현재 자산: {capital:,.0f}원\n"
            f"일일 수익률: {daily_return_pct:+.2f}%\n"
            f"오늘 거래: {trades_today}건"
        )
        self.send(msg)

    def notify_error(self, error_msg: str):
        msg = f"⚠️ <b>오류 발생</b>\n{error_msg}\n{datetime.now().strftime('%H:%M:%S')}"
        self.send(msg)

    def notify_halt(self, reason: str):
        msg = f"🚨 <b>거래 중단</b>\n사유: {reason}"
        self.send(msg)
