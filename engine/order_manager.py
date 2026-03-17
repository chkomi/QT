"""
주문 실행 모듈 (업비트 실전)

기능:
  - 시장가/지정가 매수·매도
  - 잔고 조회
  - 주문 상태 확인
  - 페이퍼 트레이딩 모드 (실제 주문 없이 시뮬레이션)
"""
import os
import logging
import pyupbit
from dotenv import load_dotenv
from typing import Optional

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../config/.env"))

logger = logging.getLogger(__name__)


class OrderManager:
    """
    Parameters
    ----------
    paper_trading : True이면 실제 주문 없이 로그만 출력 (기본 True)
    """

    def __init__(self, paper_trading: bool = True):
        self.paper_trading = paper_trading
        self._upbit = None

        if not paper_trading:
            access = os.getenv("UPBIT_ACCESS_KEY")
            secret = os.getenv("UPBIT_SECRET_KEY")
            if not access or not secret:
                raise ValueError(
                    ".env 파일에 UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY를 입력해주세요.\n"
                    "config/.env.example 파일을 참고하세요."
                )
            self._upbit = pyupbit.Upbit(access, secret)
            logger.info("[OrderManager] 실전 모드 — 업비트 연결 완료")
        else:
            logger.info("[OrderManager] 페이퍼 트레이딩 모드 (가상 주문)")

    # ── 잔고 조회 ──────────────────────────────────────────

    def get_balance_krw(self) -> float:
        """KRW 잔고 조회"""
        if self.paper_trading:
            return 0.0
        try:
            return self._upbit.get_balance("KRW")
        except Exception as e:
            logger.error(f"KRW 잔고 조회 실패: {e}")
            return 0.0

    def get_balance_coin(self, market: str) -> float:
        """코인 잔고 조회 (예: 'KRW-BTC' → BTC 수량)"""
        if self.paper_trading:
            return 0.0
        ticker = market.split("-")[1]
        try:
            return self._upbit.get_balance(ticker)
        except Exception as e:
            logger.error(f"{ticker} 잔고 조회 실패: {e}")
            return 0.0

    def get_avg_buy_price(self, market: str) -> float:
        """평균 매수가 조회"""
        if self.paper_trading:
            return 0.0
        ticker = market.split("-")[1]
        try:
            return self._upbit.get_avg_buy_price(ticker)
        except Exception as e:
            logger.error(f"평균 매수가 조회 실패: {e}")
            return 0.0

    # ── 매수 ───────────────────────────────────────────────

    def buy_market_order(self, market: str, amount_krw: float) -> Optional[dict]:
        """
        시장가 매수

        Parameters
        ----------
        market     : "KRW-BTC" | "KRW-ETH"
        amount_krw : 매수 금액 (원)

        Returns
        -------
        주문 결과 딕셔너리 | None (실패 시)
        """
        if amount_krw < 10_000:
            logger.warning(f"최소 주문 금액(10,000원) 미달: {amount_krw:,.0f}원")
            return None

        if self.paper_trading:
            logger.info(f"[PAPER] 시장가 매수 | {market} | {amount_krw:,.0f}원")
            return {"type": "paper_buy", "market": market, "price": amount_krw}

        try:
            result = self._upbit.buy_market_order(market, amount_krw)
            logger.info(f"[실전] 시장가 매수 | {market} | {amount_krw:,.0f}원 | 결과: {result}")
            return result
        except Exception as e:
            logger.error(f"매수 주문 실패 ({market}): {e}")
            return None

    def buy_limit_order(self, market: str, price: float, volume: float) -> Optional[dict]:
        """
        지정가 매수

        Parameters
        ----------
        price  : 매수 희망 가격 (원)
        volume : 매수 수량 (코인)
        """
        if self.paper_trading:
            logger.info(f"[PAPER] 지정가 매수 | {market} | {price:,.0f}원 × {volume} 코인")
            return {"type": "paper_limit_buy", "market": market, "price": price, "volume": volume}

        try:
            result = self._upbit.buy_limit_order(market, price, volume)
            logger.info(f"[실전] 지정가 매수 | {market} | {price:,.0f}원 × {volume}")
            return result
        except Exception as e:
            logger.error(f"지정가 매수 실패: {e}")
            return None

    # ── 매도 ───────────────────────────────────────────────

    def sell_market_order(self, market: str, volume: float) -> Optional[dict]:
        """
        시장가 매도

        Parameters
        ----------
        volume : 매도 수량 (코인)
        """
        if self.paper_trading:
            logger.info(f"[PAPER] 시장가 매도 | {market} | {volume} 코인")
            return {"type": "paper_sell", "market": market, "volume": volume}

        try:
            result = self._upbit.sell_market_order(market, volume)
            logger.info(f"[실전] 시장가 매도 | {market} | {volume} 코인 | 결과: {result}")
            return result
        except Exception as e:
            logger.error(f"매도 주문 실패 ({market}): {e}")
            return None

    def sell_limit_order(self, market: str, price: float, volume: float) -> Optional[dict]:
        """지정가 매도"""
        if self.paper_trading:
            logger.info(f"[PAPER] 지정가 매도 | {market} | {price:,.0f}원 × {volume}")
            return {"type": "paper_limit_sell", "market": market, "price": price, "volume": volume}

        try:
            result = self._upbit.sell_limit_order(market, price, volume)
            logger.info(f"[실전] 지정가 매도 | {market} | {price:,.0f}원 × {volume}")
            return result
        except Exception as e:
            logger.error(f"지정가 매도 실패: {e}")
            return None

    # ── 주문 취소 ──────────────────────────────────────────

    def cancel_order(self, uuid: str) -> Optional[dict]:
        """주문 취소"""
        if self.paper_trading:
            logger.info(f"[PAPER] 주문 취소: {uuid}")
            return {"cancelled": uuid}

        try:
            return self._upbit.cancel_order(uuid)
        except Exception as e:
            logger.error(f"주문 취소 실패: {e}")
            return None

    def get_order(self, uuid: str) -> Optional[dict]:
        """주문 상태 조회"""
        if self.paper_trading:
            return {"uuid": uuid, "state": "done"}

        try:
            return self._upbit.get_order(uuid)
        except Exception as e:
            logger.error(f"주문 조회 실패: {e}")
            return None
