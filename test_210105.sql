WITH BASIS_DT AS (
    SELECT
          :YYYYMM AS DT
          ,TO_DATE(:YYYYMM, 'YYYYMM') AS DT_DATE
    FROM DUAL
)
         SELECT :YYYYMM AS END_DT
                ,TO_CHAR(ADD_MONTHS(DT_DATE, -1),'YYYYMM')                            AS LAST_MON_DT --지난달
                ,TO_CHAR(ADD_MONTHS(DT_DATE, -:VN_LAST_INTERVAL+1),'YYYYMM')          AS RECENT_START_DT --최근첫달
                ,TO_CHAR(TRUNC(DT_DATE, 'YY'), 'YYYYMM')                              AS RECENT_YR_START_DT --올해첫달
                ,TO_CHAR(TRUNC(DT_DATE, 'Q'),'YYYYMM')                                AS QTR_START_DT --분기첫달
                ,TO_CHAR(TRUNC(TRUNC(DT_DATE, 'Q')-1,'Q'),'YYYYMM')                   AS LAST_QTR_START_DT --지난분기첫달
                ,TO_CHAR(TRUNC(DT_DATE, 'Q')-1,'YYYYMM')                              AS LAST_QTR_END_DT --지난분기막달
                ,TO_CHAR(TRUNC(TRUNC(DT_DATE, 'IY'), 'YY'),'YYYYMM')                  AS LAST_YR_START_DT --지난해첫달
                ,TO_CHAR(ADD_MONTHS(DT_DATE, -12),'YYYYMM')                           AS LAST_YR_SAME_DT --지난해동월
                ,TO_CHAR(TRUNC(DT_DATE, 'IY'),'YYYYMM')                               AS LAST_YR_END_DT --지난해막달
                ,TO_CHAR(TRUNC(ADD_MONTHS(DT_DATE, -12), 'Q'),'YYYYMM')               AS LAST_YR_QTR_START_DT --작년분기첫달
                ,TO_CHAR(ADD_MONTHS(TRUNC(ADD_MONTHS(DT_DATE, -12), 'Q'),2),'YYYYMM') AS LAST_YR_QTR_END_DT --작년분기막달
         FROM BASIS_DT
        )

SELECT T1.*
      ,T1.AVG / T1.STD AS ABC
FROM(
        SELECT :YYYYMM YYYYMM
              ,T1.*
              ,SUM(T1.SALES) OVER(PARTITION BY T1.MS_GROUP)
              ,SUM(T1.QTR_SALES) OVER(PARTITION BY T1.MS_GROUP)
              ,SUM(T1.LAST_MON_SALES) OVER(PARTITION BY T1.CUST_ID)
              ,STDDEV(T1.SALES) OVER(PARTITION BY T1.MS_GROUP) AS STD
              ,AVG(T1.SALES) OVER(PARTITION BY T1.MS_GROUP) AS AVG
              ,COUNT(T1.RECENT_SALES) OVER(PARTITION BY T1.MS_GROUP) AS CNT
        FROM (
        --------------------------------------------------------------------------------------------------------------------------------------------------
                SELECT MS_ID
                      ,SITE_ID
                      ,CUST_ID
                      ,MS_GROUP
                      ,SUM(CASE WHEN YYYYMM = DT.END_DT THEN QUANTITY ELSE 0 END) AS SALES --당월판매
                      ,SUM(CASE WHEN YYYYMM = DT.LAST_MON_DT THEN QUANTITY ELSE 0 END) AS LAST_MON_SALES --지난달판매
                      ,SUM(CASE WHEN YYYYMM >= DT.RECENT_START_DT    AND YYYYMM <= DT.END_DT THEN QUANTITY ELSE 0 END) AS RECENT_SALES --연간최근판매
                      ,SUM(CASE WHEN YYYYMM >= DT.QTR_START_DT       AND YYYYMM <= DT.END_DT THEN QUANTITY ELSE 0 END) AS QTR_SALES -- 분기판매
                      ,SUM(CASE WHEN YYYYMM >= DT.LAST_QTR_START_DT  AND YYYYMM <= DT.LAST_QTR_END_DT THEN QUANTITY ELSE 0 END) AS LAST_QTR_SALES --지난분기판매
                      ,SUM(CASE WHEN YYYYMM >= DT.RECENT_YR_START_DT AND YYYYMM <= DT.END_DT THEN QUANTITY ELSE 0 END) AS YR_SALES --올해최근
                FROM FD_DP_MNLY_SHIPMNT, DT
                WHERE YYYYMM BETWEEN LAST_YR_START_DT AND DT.END_DT
                GROUP BY MS_ID
                      ,SITE_ID
                      ,CUST_ID
                      ,MS_GROUP
        --------------------------------------------------------------------------------------------------------------------------------------------------
              ) T1
)T1

SELECT *
FROM FD_DP_MNLY_SHIPMNT
WHERE YYYYMM BETWEEN '201812' AND '201902' AND MS_GROUP = 'Backseal'
