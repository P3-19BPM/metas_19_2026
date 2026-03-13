SELECT
    MAX(
        COALESCE(
            OCO.data_hora_primeiro_fim_preenchimento,
            OCO.data_hora_alteracao,
            OCO.data_hora_fechamento,
            OCO.data_hora_inclusao,
            OCO.data_hora_fato
        )
    ) AS ultima_referencia_incremental
FROM
    db_bisp_reds_reporting.tb_ocorrencia AS OCO;
