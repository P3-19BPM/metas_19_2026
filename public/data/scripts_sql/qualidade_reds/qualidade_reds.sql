--NAME: QUALIDADE_REDS
WITH envolvidos_flags AS (
    SELECT
        ENV.numero_ocorrencia,
        MAX(
            CASE
                WHEN
                    REGEXP_REPLACE(LOWER(TRIM(COALESCE(ENV.numero_cpf_cnpj, ''))), '[^a-z0-9]', '') = ''
                    OR REGEXP_EXTRACT(REGEXP_REPLACE(LOWER(TRIM(COALESCE(ENV.numero_cpf_cnpj, ''))), '[^a-z0-9]', ''), '^x+$', 0) <> ''
                    OR REGEXP_EXTRACT(REGEXP_REPLACE(COALESCE(ENV.numero_cpf_cnpj, ''), '[^0-9]', ''), '^0+$', 0) <> ''
                    OR LENGTH(REGEXP_REPLACE(COALESCE(ENV.numero_cpf_cnpj, ''), '[^0-9]', '')) < 11
                THEN 1 ELSE 0
            END
        ) AS flag_envolvido_sem_cpf,
        MAX(
            CASE
                WHEN
                    REGEXP_REPLACE(LOWER(TRIM(COALESCE(ENV.numero_documento_id, ''))), '[^a-z0-9]', '') = ''
                    OR REGEXP_EXTRACT(REGEXP_REPLACE(LOWER(TRIM(COALESCE(ENV.numero_documento_id, ''))), '[^a-z0-9]', ''), '^x+$', 0) <> ''
                    OR REGEXP_EXTRACT(REGEXP_REPLACE(LOWER(TRIM(COALESCE(ENV.numero_documento_id, ''))), '[^a-z0-9]', ''), '^0+$', 0) <> ''
                THEN 1 ELSE 0
            END
        ) AS flag_envolvido_sem_rg,
        MAX(
            CASE
                WHEN
                    REGEXP_REPLACE(LOWER(TRIM(COALESCE(ENV.email, ''))), '[^a-z0-9@._-]', '') = ''
                    OR REGEXP_EXTRACT(REGEXP_REPLACE(LOWER(TRIM(COALESCE(ENV.email, ''))), '[^a-z0-9@._-]', ''), '^x+$', 0) <> ''
                    OR REGEXP_EXTRACT(REGEXP_REPLACE(LOWER(TRIM(COALESCE(ENV.email, ''))), '[^a-z0-9@._-]', ''), '^0+$', 0) <> ''
                    OR INSTR(LOWER(TRIM(COALESCE(ENV.email, ''))), '@') = 0
                THEN 1 ELSE 0
            END
        ) AS flag_envolvido_sem_email,
        MAX(
            CASE
                WHEN
                    (
                        REGEXP_REPLACE(COALESCE(ENV.numero_telefone_residencial, ''), '[^0-9]', '') = ''
                        OR REGEXP_EXTRACT(REGEXP_REPLACE(COALESCE(ENV.numero_telefone_residencial, ''), '[^0-9]', ''), '^0+$', 0) <> ''
                        OR REGEXP_EXTRACT(REGEXP_REPLACE(LOWER(TRIM(COALESCE(ENV.numero_telefone_residencial, ''))), '[^a-z0-9]', ''), '^x+$', 0) <> ''
                        OR LENGTH(REGEXP_REPLACE(COALESCE(ENV.numero_telefone_residencial, ''), '[^0-9]', '')) < 8
                    )
                    AND
                    (
                        REGEXP_REPLACE(COALESCE(ENV.numero_telefone_comercial, ''), '[^0-9]', '') = ''
                        OR REGEXP_EXTRACT(REGEXP_REPLACE(COALESCE(ENV.numero_telefone_comercial, ''), '[^0-9]', ''), '^0+$', 0) <> ''
                        OR REGEXP_EXTRACT(REGEXP_REPLACE(LOWER(TRIM(COALESCE(ENV.numero_telefone_comercial, ''))), '[^a-z0-9]', ''), '^x+$', 0) <> ''
                        OR LENGTH(REGEXP_REPLACE(COALESCE(ENV.numero_telefone_comercial, ''), '[^0-9]', '')) < 8
                    )
                THEN 1 ELSE 0
            END
        ) AS flag_envolvido_sem_telefone
    FROM db_bisp_reds_reporting.tb_envolvido_ocorrencia AS ENV
    GROUP BY ENV.numero_ocorrencia
)
SELECT
    OCO.numero_ocorrencia,
    CASE
        WHEN OCO.unidade_responsavel_registro_nome LIKE '%74 CIA TM%' THEN '74 CIA TM'
        WHEN OCO.unidade_responsavel_registro_nome LIKE '%42 CIA PM%' THEN '42 CIA PM'
        WHEN OCO.unidade_responsavel_registro_nome LIKE '%47 CIA PM%' THEN '47 CIA PM'
        WHEN OCO.unidade_responsavel_registro_nome LIKE '%155 CIA PM%' THEN '155 CIA PM'
        WHEN OCO.unidade_responsavel_registro_nome LIKE '%232 CIA PM%' THEN '232 CIA PM'
        WHEN OCO.unidade_area_militar_nome LIKE '%74 CIA TM%' THEN '74 CIA TM'
        WHEN OCO.unidade_area_militar_nome LIKE '%42 CIA PM%' THEN '42 CIA PM'
        WHEN OCO.unidade_area_militar_nome LIKE '%47 CIA PM%' THEN '47 CIA PM'
        WHEN OCO.unidade_area_militar_nome LIKE '%155 CIA PM%' THEN '155 CIA PM'
        WHEN OCO.unidade_area_militar_nome LIKE '%232 CIA PM%' THEN '232 CIA PM'
        ELSE OCO.unidade_responsavel_registro_nome
    END AS cia_pm,
    CASE
        WHEN OCO.unidade_responsavel_registro_nome LIKE '%74 CIA TM/19%' THEN 'PEL/74 CIA TM/19 BPM/15 RPM'
        WHEN OCO.nome_municipio = 'TEOFILO OTONI'
            AND OCO.nome_bairro IN ('CENTRO', 'CIDADE ALTA', 'MARAJOARA', 'DR LAERTE LAENDER', 'OLGA PRATES CORREA', 'FILADÃƒâ€°LFIA', 'SÃƒÆ’O DIOGO', 'SAO DIOGO', 'FILADELFIA', 'DOUTOR LAERTE LAENDER') THEN '1 PEL/47 CIA PM/19 BPM'
        WHEN OCO.nome_municipio = 'TEOFILO OTONI'
            AND OCO.nome_bairro IN ('IPIRANGA', 'ALTINO BARBOSA', 'SÃƒÆ’O FRANCISCO', 'SAO FRANCISCO', 'TABAJARAS', 'GRÃƒÆ’O PARÃƒÂ', 'GRAO PARA', 'FÃƒÂTIMA', 'FATIMA', 'JARDIM IRACEMA', 'SANTA CLARA', 'VILA SÃƒÆ’O JOÃƒÆ’O', 'VILA SAO JOAO', 'TURMA 37', 'BELVEDERE', 'CASTRO PIRES', 'RESIDENCIAL GRAN VIVER', 'GRAN VIVER', 'FRIMUSA') THEN '2 PEL/47 CIA PM/19 BPM'
        WHEN OCO.nome_municipio = 'TEOFILO OTONI'
            AND OCO.nome_bairro IN ('MANOEL PIMENTA', 'FREI JÃƒÅ¡LIO', 'FREI JULIO', 'TEÃƒâ€œFILO ROCHA', 'TEOFILO ROCHA', 'FUNCIONÃƒÂRIOS', 'FUNCIONARIOS', 'MUCURI', 'LOURIVAL SOARES DA COSTA', 'ESPERANÃƒâ€¡A', 'ESPERANCA', 'SOLIDARIEDADE', 'AEROPORTO', 'VILA BARREIROS', 'SÃƒÆ’O BENEDITO', 'SAO BENEDITO', 'SANTO ANTÃƒâ€NIO', 'SANTO ANTONIO', 'JARDIM SÃƒÆ’O PAULO', 'JARDIM SAO PAULO', 'CIDADE NOVA') THEN '3 PEL/47 CIA PM/19 BPM'
        WHEN OCO.nome_municipio = 'TEOFILO OTONI'
            AND OCO.nome_bairro IN ('SÃƒÆ’O CRISTÃƒâ€œVÃƒÆ’O', 'SAO CRISTOVAO', 'VIRIATO', 'JOAQUIM PEDROSA', 'GANGORRINHA', 'CONCÃƒâ€œRDIA', 'CONCORDIA', 'MINAS NOVA', 'FREI DIMAS', 'PALMEIRAS', 'SÃƒÆ’O DIOGO', 'SAO DIOGO', 'INDAIA', 'INDAIÃƒÂ', 'JARDIM DAS ACÃƒÂCIAS', 'JARDIM DAS ACACIAS') THEN '1 PEL/42 CIA PM/19 BPM'
        WHEN OCO.nome_municipio = 'TEOFILO OTONI'
            AND OCO.nome_bairro IN ('ITAGUAÃƒâ€¡U', 'ITAGUACU', 'BELA VISTA', 'VILA BETEL', 'FELICIDADE', 'JARDIM FLORESTA', 'JARDIM SERRA VERDE', 'MATINHA', 'MONTE CARLO', 'NOVO HORIZONTE', 'PAMPULHINHA', 'RESIDENCIAL', 'LARANJEIRAS', 'JARDIM FLORESTA', 'SÃƒÆ’O JACINTO', 'SAO JACINTO') THEN '2 PEL/42 CIA PM/19 BPM'
        WHEN OCO.nome_municipio = 'NOVO CRUZEIRO' THEN '1 PEL/232 CIA PM/19 BPM'
        WHEN OCO.nome_municipio IN ('CARAI', 'CARAÃƒÂ', 'CATUJI', 'ITAIPE', 'ITAIPÃƒâ€°') THEN '2 PEL/232 CIA PM/19 BPM'
        WHEN OCO.nome_municipio IN ('FRANCISCOPOLIS', 'FRANCISCÃƒâ€œPOLIS', 'LADAINHA', 'MALACACHETA', 'POTE', 'POTÃƒâ€°', 'SETUBINHA') THEN '3 PEL/232 CIA PM/19 BPM'
        WHEN OCO.nome_municipio IN ('CAMPANARIO', 'CAMPANÃƒÂRIO', 'FREI GASPAR', 'ITAMBACURI', 'JAMPRUCA', 'NOVA MODICA', 'NOVA MÃƒâ€œDICA', 'PESCADOR', 'SAO JOSE DO DIVINO', 'SÃƒÆ’O JOSÃƒâ€° DO DIVINO') THEN '1 PEL/155 CIA PM/19 BPM'
        WHEN OCO.nome_municipio IN ('ATALEIA', 'ATALÃƒâ€°IA', 'OURO VERDE DE MINAS') THEN '2 PEL/155 CIA PM/19 BPM'
        WHEN OCO.nome_municipio IN ('NOVO ORIENTE DE MINAS', 'PAVAO', 'PAVÃƒÆ’O') THEN '3 PEL/42 CIA PM/19 BPM'
        ELSE '3 PEL/42 CIA PM/19 BPM'
    END AS pelotao_pm,
    OCO.data_hora_fato AS data_hora_fato,
    OCO.data_hora_inclusao AS data_hora_inclusao,
    OCO.data_hora_final AS data_hora_final,
    CAST(OCO.data_hora_fato AS DATE) AS data_fato,
    YEAR(OCO.data_hora_fato) AS ano,
    MONTH(OCO.data_hora_fato) AS mes,
    OCO.data_hora_fechamento,
    OCO.ind_estado,
    OCO.descricao_estado,
    OCO.nome_tipo_relatorio,
    OCO.natureza_codigo,
    OCO.natureza_descricao,
    OCO.natureza_secundaria1_codigo,
    OCO.natureza_secundaria1_descricao,
    OCO.natureza_secundaria2_codigo,
    OCO.natureza_secundaria2_descricao,
    OCO.natureza_secundaria3_codigo,
    OCO.natureza_secundaria3_descricao,
    OCO.data_hora_alteracao,
    OCO.data_hora_primeiro_fim_preenchimento,
    OCO.digitador_cargo_efetivo,
    OCO.digitador_matricula,
    OCO.digitador_nome,
    OCO.digitador_nome_unidade,
    OCO.historico_ocorrencia,
    COALESCE(ENVF.flag_envolvido_sem_cpf, 1) AS flag_envolvido_sem_cpf,
    COALESCE(ENVF.flag_envolvido_sem_rg, 1) AS flag_envolvido_sem_rg,
    COALESCE(ENVF.flag_envolvido_sem_email, 1) AS flag_envolvido_sem_email,
    COALESCE(ENVF.flag_envolvido_sem_telefone, 1) AS flag_envolvido_sem_telefone
FROM db_bisp_reds_reporting.tb_ocorrencia AS OCO
LEFT JOIN envolvidos_flags AS ENVF
    ON ENVF.numero_ocorrencia = OCO.numero_ocorrencia
WHERE 1=1
    AND (OCO.nome_tipo_relatorio IN ('POLICIAL', 'AMBIENTAL', 'TRANSITO') OR OCO.natureza_codigo IN ('W01000', 'W02000', 'W03000', 'W04000'))
    AND OCO.unidade_responsavel_registro_nome LIKE '%19 BPM%'
    -- Filtro dinamico de janela (injetado pelo pipeline): incremental_12d ou full_2026.
    AND {{QUALITY_DATE_FILTER}}
    AND OCO.digitador_sigla_orgao = 'PM';
