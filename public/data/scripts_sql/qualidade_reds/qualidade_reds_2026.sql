--NAME: QUALIDADE_REDS
	SELECT 
		OCO.numero_ocorrencia ,
		CASE
            WHEN OCO.unidade_responsavel_registro_nome LIKE '%74 CIA TM%'  THEN '74 CIA TM'
            WHEN OCO.unidade_responsavel_registro_nome LIKE '%42 CIA PM%' THEN '42 CIA PM' 
            WHEN OCO.unidade_responsavel_registro_nome LIKE '%47 CIA PM%' THEN '47 CIA PM' 
            WHEN OCO.unidade_responsavel_registro_nome LIKE '%155 CIA PM%' THEN '155 CIA PM' 
            WHEN OCO.unidade_responsavel_registro_nome LIKE '%232 CIA PM%' THEN '232 CIA PM' 
            WHEN OCO.unidade_area_militar_nome LIKE '%74 CIA TM%'  THEN '74 CIA TM'
            WHEN OCO.unidade_area_militar_nome LIKE '%42 CIA PM%' THEN '42 CIA PM' 
            WHEN OCO.unidade_area_militar_nome LIKE '%47 CIA PM%' THEN '47 CIA PM' 
            WHEN OCO.unidade_area_militar_nome LIKE '%155 CIA PM%' THEN '155 CIA PM' 
            WHEN OCO.unidade_area_militar_nome LIKE '%232 CIA PM%' THEN '232 CIA PM'  
            ELSE OCO.unidade_responsavel_registro_nome
     	  END  AS cia_pm,
	      CASE
            WHEN OCO.unidade_responsavel_registro_nome LIKE '%74 CIA TM/19%' THEN 'PEL/74 CIA TM/19 BPM/15 RPM'
            WHEN OCO.nome_municipio = 'TEOFILO OTONI' 
                AND OCO.nome_bairro IN ('CENTRO', 'CIDADE ALTA', 'MARAJOARA', 'DR LAERTE LAENDER', 'OLGA PRATES CORREA', 'FILADÃ‰LFIA', 'SÃƒO DIOGO', 'SAO DIOGO', 'FILADELFIA', 'DOUTOR LAERTE LAENDER') THEN '1 PEL/47 CIA PM/19 BPM'
            WHEN OCO.nome_municipio = 'TEOFILO OTONI' 
                AND OCO.nome_bairro IN ('IPIRANGA', 'ALTINO BARBOSA', 'SÃƒO FRANCISCO', 'SAO FRANCISCO', 'TABAJARAS', 'GRÃƒO PARÃ', 'GRAO PARA', 'FÃTIMA', 'FATIMA', 'JARDIM IRACEMA', 'SANTA CLARA', 'VILA SÃƒO JOÃƒO', 'VILA SAO JOAO', 'TURMA 37', 'BELVEDERE', 'CASTRO PIRES', 'RESIDENCIAL GRAN VIVER', 'GRAN VIVER', 'FRIMUSA') THEN '2 PEL/47 CIA PM/19 BPM'
            WHEN OCO.nome_municipio = 'TEOFILO OTONI' 
                AND OCO.nome_bairro IN ('MANOEL PIMENTA', 'FREI JÃšLIO', 'FREI JULIO', 'TEÃ“FILO ROCHA', 'TEOFILO ROCHA', 'FUNCIONÃRIOS', 'FUNCIONARIOS', 'MUCURI', 'LOURIVAL SOARES DA COSTA', 'ESPERANÃ‡A', 'ESPERANCA', 'SOLIDARIEDADE', 'AEROPORTO', 'VILA BARREIROS', 'SÃƒO BENEDITO', 'SAO BENEDITO', 'SANTO ANTÃ”NIO', 'SANTO ANTONIO', 'JARDIM SÃƒO PAULO', 'JARDIM SAO PAULO', 'CIDADE NOVA') THEN '3 PEL/47 CIA PM/19 BPM'
            WHEN OCO.nome_municipio = 'TEOFILO OTONI' 
                AND OCO.nome_bairro IN ('SÃƒO CRISTÃ“VÃƒO', 'SAO CRISTOVAO', 'VIRIATO', 'JOAQUIM PEDROSA', 'GANGORRINHA', 'CONCÃ“RDIA', 'CONCORDIA', 'MINAS NOVA', 'FREI DIMAS', 'PALMEIRAS', 'SÃƒO DIOGO', 'SAO DIOGO', 'INDAIA', 'INDAIÃ', 'JARDIM DAS ACÃCIAS', 'JARDIM DAS ACACIAS') THEN '1 PEL/42 CIA PM/19 BPM'
            WHEN OCO.nome_municipio = 'TEOFILO OTONI' 
                AND OCO.nome_bairro IN ('ITAGUAÃ‡U', 'ITAGUACU', 'BELA VISTA', 'VILA BETEL', 'FELICIDADE', 'JARDIM FLORESTA', 'JARDIM SERRA VERDE', 'MATINHA', 'MONTE CARLO', 'NOVO HORIZONTE', 'PAMPULHINHA', 'RESIDENCIAL', 'LARANJEIRAS', 'JARDIM FLORESTA', 'SÃƒO JACINTO', 'SAO JACINTO') THEN '2 PEL/42 CIA PM/19 BPM'
            WHEN OCO.nome_municipio = 'NOVO CRUZEIRO' THEN '1 PEL/232 CIA PM/19 BPM'
            WHEN OCO.nome_municipio IN ('CARAI', 'CARAÃ', 'CATUJI', 'ITAIPE', 'ITAIPÃ‰') THEN '2 PEL/232 CIA PM/19 BPM'
            WHEN OCO.nome_municipio IN ('FRANCISCOPOLIS', 'FRANCISCÃ“POLIS', 'LADAINHA', 'MALACACHETA', 'POTE', 'POTÃ‰', 'SETUBINHA') THEN '3 PEL/232 CIA PM/19 BPM'
            WHEN OCO.nome_municipio IN ('CAMPANARIO', 'CAMPANÃRIO', 'FREI GASPAR', 'ITAMBACURI', 'JAMPRUCA', 'NOVA MODICA', 'NOVA MÃ“DICA', 'PESCADOR', 'SAO JOSE DO DIVINO', 'SÃƒO JOSÃ‰ DO DIVINO') THEN '1 PEL/155 CIA PM/19 BPM'
            WHEN OCO.nome_municipio IN ('ATALEIA', 'ATALÃ‰IA', 'OURO VERDE DE MINAS') THEN '2 PEL/155 CIA PM/19 BPM'
            WHEN OCO.nome_municipio IN ('NOVO ORIENTE DE MINAS', 'PAVAO', 'PAVÃƒO') THEN '3 PEL/42 CIA PM/19 BPM'
            ELSE '3 PEL/42 CIA PM/19 BPM'
        END AS PELOTAO_PM,
        OCO.data_hora_fato AS data_hora_fato,
        OCO.data_hora_inclusao AS data_hora_inclusao,
        OCO.data_hora_final AS data_hora_final,
        CAST(OCO.data_hora_fato AS DATE) AS data_fato,
        YEAR(OCO.data_hora_fato) AS ano,
        MONTH(OCO.data_hora_fato) AS mes,
        OCO.data_hora_fechamento ,
        OCO.ind_estado,  -- Status tecnico da ocorrencia (A/F/R/N)
        OCO.descricao_estado,  -- Estado da ocorrÃªncia (ex: Fechado, Aberto)
        OCO.nome_tipo_relatorio,  -- Tipo de relatÃ³rio (ex: Policial, REFAP, TrÃ¢nsito)
        OCO.natureza_codigo,  -- CÃ³digo da natureza principal da ocorrÃªncia
        OCO.natureza_descricao,  -- DescriÃ§Ã£o da natureza principal
        OCO.natureza_secundaria1_codigo,  -- CÃ³digo da primeira natureza secundÃ¡ria
        OCO.natureza_secundaria1_descricao,  -- DescriÃ§Ã£o da primeira natureza secundÃ¡ria
        OCO.natureza_secundaria2_codigo,  -- CÃ³digo da segunda natureza secundÃ¡ria
        OCO.natureza_secundaria2_descricao,  -- DescriÃ§Ã£o da segunda natureza secundÃ¡ria
        OCO.natureza_secundaria3_codigo,  -- CÃ³digo da terceira natureza secundÃ¡ria
        OCO.natureza_secundaria3_descricao,  -- DescriÃ§Ã£o da terceira natureza secundÃ¡ria      
        OCO.data_hora_alteracao ,
        OCO.data_hora_primeiro_fim_preenchimento ,
        OCO.digitador_cargo_efetivo ,
        OCO.digitador_matricula ,
        OCO.digitador_nome ,
        OCO.digitador_nome_unidade ,
        OCO.historico_ocorrencia 
	FROM
		db_bisp_reds_reporting.tb_ocorrencia AS OCO
	WHERE 1=1
		AND (OCO.nome_tipo_relatorio IN ('POLICIAL', 'AMBIENTAL', 'TRANSITO') OR OCO.natureza_codigo IN ('W01000', 'W02000', 'W03000', 'W04000'))
		AND OCO.unidade_responsavel_registro_nome LIKE '%19 BPM%'
		-- Filtro fixo: ano 2026 (01/01/2026 ate hoje).
		AND OCO.data_hora_fato >= CAST('2026-01-01' AS TIMESTAMP) AND OCO.data_hora_fato < CAST(date_add(current_date(), 1) AS TIMESTAMP)
		AND OCO.digitador_sigla_orgao = 'PM';
	


