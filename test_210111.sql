WITH BASED_DT AS
(
    SELECT :YYYYMM AS DT
           ,TO_DATE(:YYYYMM,'YYYYMM') AS DT_DATE
    FROM DUAL
), DT AS(
              SELECT :YYYYMM                                                               AS END_DT
                     ,TO_CHAR(ADD_MONTHS(DT_DATE,-1),'YYYYMM')                             AS LAST_MON_DT
                     ,TO_CHAR(ADD_MONTHS(DT_DATE, -:RECENT_INTERVAL+1),'YYYYMM')           AS RECENT_START_DT
                     ,TO_CHAR(TRUNC(DT_DATE, 'YY'), 'YYYYMM')                              AS RECENT_YR_START_DT
                     ,TO_CHAR(TRUNC(DT_DATE, 'Q'), 'YYYYMM')                               AS QTR_START_DT
                     ,TO_CHAR(TRUNC(TRUNC(DT_DATE,'Q')-1,'Q'),'YYYYMM')                    AS LAST_QTR_START_DT
                     ,TO_CHAR(TRUNC(DT_DATE,'Q')-1,'YYYYMM')                               AS LAST_QTR_END_DT
                     ,TO_CHAR(TRUNC(ADD_MONTHS(DT_DATE, -12),'YY'),'YYYYMM')               AS LAST_YR_START_DT
                     ,TO_CHAR(ADD_MONTHS(DT_DATE, -12),'YYYYMM')                           AS LAST_YR_SAME_DT
                     ,TO_CHAR(TRUNC(DT_DATE, 'YY')-1, 'YYYYMM')                            AS LAST_YR_END_DT
                     ,TO_CHAR(TRUNC(ADD_MONTHS(DT_DATE,-12),'Q'), 'YYYYMM')                AS LAST_YR_QTR_START_DT
                     ,TO_CHAR(ADD_MONTHS(TRUNC(ADD_MONTHS(DT_DATE,-12),'Q'),2), 'YYYYMM')  AS LAST_YR_QTR_END_DT
              FROM BASED_DT
        )

SELECT  T1.당월판매량
       ,T1.전월판매량
       ,T1.최근판매량
       ,T1.분기판매량
       ,T1.연간최근판매량
       ,T1.제품당월판매량
       ,T1.고객전월판매량
       ,T1.제품전월판매량
       ,T1.CNT
       ,CASE WHEN T1.전월판매량 = 0 THEN 0 ELSE ROUND(ABS((T1.당월판매량 - T1.전월판매량) / T1.전월판매량 * 100),3) END AS 전월증가량
       ,CASE WHEN T1.전년동월판매량 = 0 THEN 0 ELSE ROUND(ABS((T1.당월판매량 - T1.전년동월판매량) / T1.전년동월판매량 * 100),3) END AS 전년동월증가량
       ,CASE WHEN T1.전월분기판매량 = 0 THEN 0 ELSE ROUND(ABS((T1.분기판매량 - T1.전월분기판매량) / T1.전월분기판매량 * 100),3) END AS 전월분기증가량
       ,CASE WHEN T1.고객전월판매량 = 0 THEN 0 ELSE ROUND(ABS((T1.고객당월판매량 - T1.고객전월판매량) / T1.고객전월판매량 * 100),3) END AS 고객전월증가량
       ,CASE WHEN T1.제품전월판매량 = 0 THEN 0 ELSE ROUND(ABS((T1.제품당월판매량 - T1.제품전월판매량) / T1.제품전월판매량 * 100),3) END AS 제품전월증가량
       ,CASE WHEN T1.STD = 0 THEN 0 ELSE ROUND(T1.AVG / T1.STD,3) END AS 변동계수
FROM(       
        SELECT :YYYYMM YYYYMM
               ,T1.*
               ,SUM(T1.당월판매량) OVER(PARTITION BY T1.MS_GROUP) 제품당월판매량
               ,SUM(T1.전월판매량) OVER(PARTITION BY T1.MS_GROUP) 제품전월판매량
               ,SUM(T1.당월판매량) OVER(PARTITION BY T1.CUST_ID) 고객당월판매량
               ,SUM(T1.전월판매량) OVER(PARTITION BY T1.CUST_ID) 고객전월판매량
               ,STDDEV(T1.당월판매량) OVER(PARTITION BY T1.MS_GROUP) AS STD
               ,AVG(T1.당월판매량) OVER(PARTITION BY T1.MS_GROUP) AS AVG
               ,COUNT(T1.연간최근판매량) OVER(PARTITION BY T1.MS_GROUP) AS CNT
        FROM (
                --------------------------------------------------------------------------------------------------------------------
                SELECT MS_ID
                      ,SITE_ID
                      ,CUST_ID
                      ,MS_GROUP
                      ,SUM(CASE WHEN YYYYMM = DT.END_DT THEN QUANTITY ELSE 0 END) AS 당월판매량
                      ,SUM(CASE WHEN YYYYMM = DT.LAST_MON_DT THEN QUANTITY ELSE 0 END) AS 전월판매량
                      ,SUM(CASE WHEN YYYYMM = LAST_YR_SAME_DT THEN QUANTITY ELSE 0 END) AS 전년동월판매량
                      ,SUM(CASE WHEN YYYYMM >= DT.RECENT_START_DT    AND YYYYMM <= DT.END_DT THEN QUANTITY ELSE 0 END) AS 최근판매량
                      ,SUM(CASE WHEN YYYYMM >= DT.QTR_START_DT       AND YYYYMM <= DT.END_DT THEN QUANTITY ELSE 0 END) AS 분기판매량
                      ,SUM(CASE WHEN YYYYMM >= DT.LAST_QTR_START_DT  AND YYYYMM <= DT.LAST_QTR_END_DT THEN QUANTITY ELSE 0 END) AS 전분기판매량
                      ,SUM(CASE WHEN YYYYMM >= DT.RECENT_YR_START_DT AND YYYYMM <= DT.END_DT THEN QUANTITY ELSE 0 END) AS 연간최근판매량
                FROM FD_DP_MNLY_SHIPMNT, DT
                WHERE YYYYMM BETWEEN LAST_YR_START_DT AND DT.END_DT -- 작년 1월부터 현재까지 조회
                GROUP BY MS_ID
                        ,SITE_ID
                        ,CUST_ID
                        ,MS_GROUP
        -----------------------------------------------------------------------------------------------------------------------------------------
        ) T1
    )T1