"""
전략 추상 기반 클래스
모든 전략은 BaseStrategy를 상속받아 generate_signals() 를 구현해야 합니다.
"""
from abc import ABC, abstractmethod
import pandas as pd


class BaseStrategy(ABC):
    """
    전략 추상 기반 클래스

    Attributes
    ----------
    name   : 전략 이름
    params : 전략 파라미터 딕셔너리
    """

    def __init__(self, name: str, params: dict):
        self.name = name
        self.params = params

    @abstractmethod
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        OHLCV 데이터를 받아 매매 신호가 추가된 DataFrame 반환

        Parameters
        ----------
        df : OHLCV DataFrame (index=datetime, columns=[open,high,low,close,volume])

        Returns
        -------
        DataFrame with additional columns:
            signal  : 1 (매수), -1 (매도), 0 (홀드)
            position: 현재 포지션 상태 (0 or 1)
        """
        pass

    def __repr__(self):
        return f"{self.__class__.__name__}({self.params})"
