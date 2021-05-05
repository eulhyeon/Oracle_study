/* SELECT *
FROM FD_DP_MNLY_SHIPMNT
WHERE YYYYMM BETWEEN '201807' AND '201906'*/

WITH BASED_DT AS
(
    SELECT :YYYYMM AS DT
           ,TO_DATE(:YYYYMM,'YYYYMM') AS DT_DATE
    FROM DUAL
)
        SELECT :YYYYMM                                    AS END_DT
                ,TO_CHAR(ADD_MONTHS(DT_DATE,-1),'YYYYMM') AS lAST_MON_DT
        FROM BASED_DT
        )
    