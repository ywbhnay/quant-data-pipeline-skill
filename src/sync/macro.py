"""宏观/指数域同步器：macro_indicators, index_dailybasic, sw_industry"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import tushare as ts
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..rate_limiter import RateLimiter
from ..retry import tushare_retry
from .base import DEFAULT_START_DATE, BaseSyncer

# 主要指数代码
_MAJOR_INDICES = [
    "000001.SH",  # 上证指数
    "399001.SZ",  # 深证成指
    "399006.SZ",  # 创业板指
    "000300.SH",  # 沪深300
    "000905.SH",  # 中证500
    "000852.SH",  # 中证1000
    "000016.SH",  # 上证50
    "000688.SH",  # 科创50
]


class MacroSyncer(BaseSyncer):
    """宏观/指数域同步

    覆盖表:
    - macro_indicators: 多接口聚合拉取宏观指标，ON CONFLICT DO UPDATE (month)
    - index_dailybasic: 按指数+日期范围拉取，ON CONFLICT DO NOTHING (ts_code, trade_date)
    - sw_industry: 全量拉取申万行业成分股快照，ON CONFLICT DO UPDATE (ts_code, trade_date)
    """

    name = "macro"

    # ------------------------------------------------------------------ #
    #  Full backfill
    # ------------------------------------------------------------------ #

    def run_full(self, session: Session) -> None:
        self._sync_macro_indicators(session)
        self._sync_index_dailybasic(session, DEFAULT_START_DATE)
        self._sync_sw_industry(session)

    # ------------------------------------------------------------------ #
    #  Incremental
    # ------------------------------------------------------------------ #

    def run_incremental(self, session: Session) -> None:
        # macro_indicators 全量重拉 (数据量小，需捕获修正)
        self._sync_macro_indicators(session)

        # index_dailybasic 从 MAX(trade_date) 之后
        today = datetime.now().strftime("%Y%m%d")
        latest = self.get_latest_date(
            session, self._model("IndexDailyBasic"), "trade_date"
        )
        start = latest and f"{int(latest) + 1:08d}" or DEFAULT_START_DATE
        if start <= today:
            self._sync_index_dailybasic(session, start)

        # sw_industry 全量重拉 (行业成分会变动)
        self._sync_sw_industry(session)

    # ------------------------------------------------------------------ #
    #  macro_indicators — 多接口聚合拉取宏观指标
    # ------------------------------------------------------------------ #

    def _sync_macro_indicators(self, session: Session) -> None:
        self.logger.info("开始同步 macro_indicators")
        pro = ts.pro_api(self._token)

        # 按子接口分别拉取，合并为按 month 索引的 dict
        all_data: dict[str, dict] = {}

        # CPI
        self.logger.info("  拉取 CPI...")
        df = self._safe_call(pro, "cn_cpi")
        if df is not None and not df.empty:
            df = df.sort_values("month").drop_duplicates(subset=["month"], keep="last")
            for _, row in df.iterrows():
                m = str(row.get("month", ""))
                if m:
                    all_data.setdefault(m, {})
                    if row.get("nt_yoy") is not None:
                        all_data[m]["cpi_yoy"] = row.get("nt_yoy")
                    if row.get("nt_mom") is not None:
                        all_data[m]["cpi_mom"] = row.get("nt_mom")
            self.logger.info("    获取 %d 条 CPI 记录", len(df))
        self.rate_limiter.acquire()

        # PPI
        self.logger.info("  拉取 PPI...")
        df = self._safe_call(pro, "cn_ppi")
        if df is not None and not df.empty:
            df = df.sort_values("month").drop_duplicates(subset=["month"], keep="last")
            for _, row in df.iterrows():
                m = str(row.get("month", ""))
                if m:
                    all_data.setdefault(m, {})
                    if row.get("ppi_yoy") is not None:
                        all_data[m]["ppi_yoy"] = row.get("ppi_yoy")
                    if row.get("ppi_mom") is not None:
                        all_data[m]["ppi_mom"] = row.get("ppi_mom")
            self.logger.info("    获取 %d 条 PPI 记录", len(df))
        self.rate_limiter.acquire()

        # PMI
        self.logger.info("  拉取 PMI...")
        df = self._safe_call(pro, "cn_pmi")
        if df is not None and not df.empty:
            df = df.sort_values("MONTH").drop_duplicates(subset=["MONTH"], keep="last")
            for _, row in df.iterrows():
                m = str(row.get("MONTH", ""))
                if m:
                    all_data.setdefault(m, {})
                    if row.get("PMI010600") is not None:
                        all_data[m]["pmi"] = row.get("PMI010600")
                    if row.get("PMI020300") is not None:
                        all_data[m]["non_pmi"] = row.get("PMI020300")
            self.logger.info("    获取 %d 条 PMI 记录", len(df))
        self.rate_limiter.acquire()

        # Shibor
        self.logger.info("  拉取 Shibor...")
        df = self._safe_call(pro, "shibor")
        if df is not None and not df.empty:
            df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last")
            for _, row in df.iterrows():
                d = str(row.get("date", ""))
                if d:
                    all_data.setdefault(d, {})
                    if row.get("on") is not None:
                        all_data[d]["shibor_on"] = row.get("on")
                    if row.get("1w") is not None:
                        all_data[d]["shibor_1w"] = row.get("1w")
                    if row.get("1m") is not None:
                        all_data[d]["shibor_1m"] = row.get("1m")
                    if row.get("3m") is not None:
                        all_data[d]["shibor_3m"] = row.get("3m")
            self.logger.info("    获取 %d 条 Shibor 记录", len(df))
        self.rate_limiter.acquire()

        # LPR
        self.logger.info("  拉取 LPR...")
        df = self._safe_call(pro, "shibor_lpr")
        if df is not None and not df.empty:
            df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last")
            for _, row in df.iterrows():
                d = str(row.get("date", ""))
                if d:
                    all_data.setdefault(d, {})
                    if row.get("1y") is not None:
                        all_data[d]["lpr_1y"] = row.get("1y")
                    if row.get("5y") is not None:
                        all_data[d]["lpr_5y"] = row.get("5y")
            self.logger.info("    获取 %d 条 LPR 记录", len(df))
        self.rate_limiter.acquire()

        # M2
        self.logger.info("  拉取 M2...")
        df = self._safe_call(pro, "cn_m")
        if df is not None and not df.empty:
            df = df.sort_values("month").drop_duplicates(subset=["month"], keep="last")
            for _, row in df.iterrows():
                m = str(row.get("month", ""))
                if m:
                    all_data.setdefault(m, {})
                    if row.get("m2_yoy") is not None:
                        all_data[m]["m2_yoy"] = row.get("m2_yoy")
            self.logger.info("    获取 %d 条 M2 记录", len(df))
        self.rate_limiter.acquire()

        # 构建统一记录列表
        model = self._model("MacroIndicators")
        table = model.__table__
        value_cols = [c.name for c in table.columns if c.name != "month"]
        records = []
        for month, values in sorted(all_data.items()):
            rec: dict = {"month": month}
            for col in value_cols:
                rec[col] = values.get(col)
            records.append(rec)

        if not records:
            self.logger.info("  无宏观数据可写入")
            return

        # ON CONFLICT DO UPDATE (宏观数据会修正)
        col_names = ", ".join(["month"] + value_cols)
        placeholders = ", ".join([f":{c}" for c in ["month"] + value_cols])
        update_assignments = ", ".join([f"{c} = EXCLUDED.{c}" for c in value_cols])
        sql = text(
            f"INSERT INTO {table.name} ({col_names}) VALUES ({placeholders}) "
            f"ON CONFLICT (month) DO UPDATE SET {update_assignments}"
        )
        session.execute(sql, records)
        self.logger.info("macro_indicators 写入 %d 条记录", len(records))

    # ------------------------------------------------------------------ #
    #  index_dailybasic — 按指数+日期范围拉取
    # ------------------------------------------------------------------ #

    def _sync_index_dailybasic(self, session: Session, start_date: str) -> None:
        self.logger.info("开始同步 index_dailybasic [%s 至今]", start_date)
        pro = ts.pro_api(self._token)

        today = datetime.now().strftime("%Y%m%d")
        total = 0

        for i, idx_code in enumerate(_MAJOR_INDICES):
            df = self._safe_call(
                pro,
                "index_dailybasic",
                ts_code=idx_code,
                start_date=start_date,
                end_date=today,
            )
            if df is not None and not df.empty:
                model = self._model("IndexDailyBasic")
                count = self.upsert_dataframe(
                    session, model, df, conflict_columns=["ts_code", "trade_date"]
                )
                total += count
                self.logger.info(
                    "index_dailybasic [%s] 写入 %d 条", idx_code, count
                )
            self.rate_limiter.acquire()

            if (i + 1) % 3 == 0:
                self.logger.info(
                    "index_dailybasic 进度 %d/%d", i + 1, len(_MAJOR_INDICES)
                )

        self.logger.info("index_dailybasic 同步完成，共写入 %d 条", total)

    # ------------------------------------------------------------------ #
    #  sw_industry — 全量拉取申万行业成分股快照
    # ------------------------------------------------------------------ #

    def _sync_sw_industry(self, session: Session) -> None:
        self.logger.info("开始同步 sw_industry")
        pro = ts.pro_api(self._token)

        df = self._safe_call(pro, "index_member_all")
        if df is None or df.empty:
            self.logger.info("  未获取到申万行业数据")
            return

        self.logger.info("  获取到 %d 条行业成员记录", len(df))

        # 过滤仍在成分股中的记录 (out_date 为空 = 当前在成分股)
        if "out_date" in df.columns:
            current_df = df[df["out_date"].isna()].copy()
        else:
            current_df = df.copy()
        self.logger.info("  当前成分股: %d 条", len(current_df))

        # 按股票去重，保留最新 (按 in_date 降序取第一条)
        if "in_date" in current_df.columns:
            current_df = current_df.sort_values("in_date", ascending=False).drop_duplicates(
                subset=["ts_code"], keep="first"
            )

        trade_date = datetime.now().strftime("%Y%m%d")

        # 构建记录
        records = []
        for _, row in current_df.iterrows():
            records.append({
                "ts_code": row.get("ts_code", ""),
                "trade_date": trade_date,
                "l1_code": row.get("l1_code"),
                "l1_name": row.get("l1_name"),
                "l2_code": row.get("l2_code"),
                "l2_name": row.get("l2_name"),
                "l3_code": row.get("l3_code"),
                "l3_name": row.get("l3_name"),
            })

        if not records:
            self.logger.info("  无有效数据")
            return

        # ON CONFLICT DO UPDATE (行业成分会变动)
        model = self._model("SwIndustry")
        table = model.__table__
        value_cols = [c.name for c in table.columns if c.name not in ("ts_code", "trade_date")]
        all_cols = ["ts_code", "trade_date"] + value_cols
        col_names = ", ".join(all_cols)
        placeholders = ", ".join([f":{c}" for c in all_cols])
        update_assignments = ", ".join([f"{c} = EXCLUDED.{c}" for c in value_cols])
        sql = text(
            f"INSERT INTO {table.name} ({col_names}) VALUES ({placeholders}) "
            f"ON CONFLICT (ts_code, trade_date) DO UPDATE SET {update_assignments}"
        )
        session.execute(sql, records)
        self.logger.info("sw_industry 写入 %d 条记录", len(records))

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    @tushare_retry
    def _safe_call(self, pro, method_name: str, **kwargs) -> pd.DataFrame | None:
        """带重试和限流的 API 调用"""
        try:
            df = getattr(pro, method_name)(**kwargs)
            if isinstance(df, pd.DataFrame) and df.empty:
                return None
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(k in error_msg for k in ("积分", "流量", "limit", "访问")):
                from ..exceptions import TushareRateLimitError

                raise TushareRateLimitError(e)
            raise

    def _model(self, name: str):
        """懒加载 ORM 模型引用，避免循环导入"""
        from ..models.macro import IndexDailyBasic, MacroIndicators, SwIndustry

        mapping = {
            "MacroIndicators": MacroIndicators,
            "IndexDailyBasic": IndexDailyBasic,
            "SwIndustry": SwIndustry,
        }
        return mapping[name]
