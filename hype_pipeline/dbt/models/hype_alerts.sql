-- models/hype_alerts.sql
-- Derives buy/sell alerts from hourly indicators.
-- Reads from a source table you define in sources.yml as:
--   sources:
--     - name: hype
--       tables:
--         - name: indicators_hourly
-- Adjust source() to your naming.

WITH base AS (
  SELECT
    ts,
    symbol,
    close,
    signal_score,
    sma_200,
    adx_14,
    rsi_14,
    bb_low_20,
    bb_up_20,
    funding_rate
  FROM {{ source('hype','indicators_hourly') }}
), derived AS (
  SELECT
    *,
    /* params */
    -2.75 AS buy_thr,
     0.75 AS sell_thr,

    /* smoothed score */
    AVG(signal_score) OVER (
      PARTITION BY symbol
      ORDER BY ts
      ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS score_smooth_window,  -- simple window smoothing (alt to EMA in SQL)

    /* BB%B */
    SAFE_DIVIDE(close - bb_low_20, NULLIF(bb_up_20 - bb_low_20, 0)) AS bb_pctB,

    /* bull regime */
    CASE WHEN close > sma_200 OR adx_14 >= 20 THEN 1 ELSE 0 END AS bull_regime,

    /* funding bps (optional) */
    funding_rate * 10000.0 AS funding_bps
  FROM base
), alerts AS (
  SELECT
    ts,
    symbol,
    close,
    signal_score,
    /* Use window-smoothed score as a proxy for EMA */
    COALESCE(score_smooth_window, signal_score) AS score_smooth,
    rsi_14,
    bb_pctB,
    funding_bps,
    bull_regime,

    /* buy */
    CASE
      WHEN COALESCE(score_smooth_window, signal_score) <= buy_thr
       AND bull_regime = 1
       AND (
         (rsi_14 <= 35) OR (bb_pctB <= 0.10)
       )
      THEN 1 ELSE 0
    END AS buy_alert,

    /* sell */
    CASE
      WHEN COALESCE(score_smooth_window, signal_score) >= sell_thr
       AND (
         (rsi_14 >= 70) OR (bb_pctB >= 0.90)
       )
      THEN 1 ELSE 0
    END AS sell_alert,

    /* confidence: 0..100 */
    (
      (GREATEST(0.0, LEAST(1.0, (sell_thr - COALESCE(score_smooth_window, signal_score)) / (sell_thr - buy_thr))) * 0.6)
      + (
          (
            IF(rsi_14 <= 35, 1, 0) +
            IF(bb_pctB <= 0.10, 1, 0)
          ) / 3.0
        ) * 0.4
    ) * 100.0 AS alert_confidence,

    /* reason text */
    CONCAT(
      CASE WHEN (COALESCE(score_smooth_window, signal_score) <= buy_thr AND bull_regime = 1) THEN 'Score<=buy_thr & bull regime; ' ELSE '' END,
      CASE WHEN rsi_14 <= 35 THEN 'RSI<=35; ' ELSE '' END,
      CASE WHEN bb_pctB <= 0.10 THEN 'BB%B<=0.10; ' ELSE '' END,
      CASE WHEN COALESCE(score_smooth_window, signal_score) >= sell_thr THEN 'Score>=sell_thr; ' ELSE '' END,
      CASE WHEN rsi_14 >= 70 THEN 'RSI>=70; ' ELSE '' END,
      CASE WHEN bb_pctB >= 0.90 THEN 'BB%B>=0.90; ' ELSE '' END
    ) AS alert_reasons
  FROM derived
), cooled AS (
  /* 12-hour cooldown per symbol per alert type */
  SELECT * FROM (
    SELECT
      a.*,
      'buy'  AS alert_type, buy_alert  AS alert_flag,
      LAG(CASE WHEN buy_alert=1 THEN ts END) OVER (PARTITION BY symbol ORDER BY ts) AS prev_buy_ts
    FROM alerts a
    UNION ALL
    SELECT
      a.*,
      'sell' AS alert_type, sell_alert AS alert_flag,
      LAG(CASE WHEN sell_alert=1 THEN ts END) OVER (PARTITION BY symbol ORDER BY ts) AS prev_buy_ts
    FROM alerts a
  )
  QUALIFY
    alert_flag = 0
    OR prev_buy_ts IS NULL
    OR TIMESTAMP_DIFF(ts, prev_buy_ts, HOUR) >= 12
), final AS (
  SELECT
    ts, symbol, close, signal_score, score_smooth, rsi_14, bb_pctB, funding_bps, bull_regime,
    MAX(buy_alert)  AS buy_alert,
    MAX(sell_alert) AS sell_alert,
    ANY_VALUE(alert_confidence) AS alert_confidence,
    ANY_VALUE(alert_reasons)    AS alert_reasons
  FROM cooled
  GROUP BY 1,2,3,4,5,6,7,8,9
)
SELECT * FROM final
ORDER BY symbol, ts;
