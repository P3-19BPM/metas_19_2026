SELECT
    MAX(OCO.data_hora_fechamento) AS ultimo_fechamento_ocorrencia
FROM
    db_bisp_reds_reporting.tb_ocorrencia AS OCO;