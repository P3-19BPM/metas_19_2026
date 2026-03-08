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
                AND OCO.nome_bairro IN ('CENTRO', 'CIDADE ALTA', 'MARAJOARA', 'DR LAERTE LAENDER', 'OLGA PRATES CORREA', 'FILADÉLFIA', 'SÃO DIOGO', 'SAO DIOGO', 'FILADELFIA', 'DOUTOR LAERTE LAENDER') THEN '1 PEL/47 CIA PM/19 BPM'
            WHEN OCO.nome_municipio = 'TEOFILO OTONI' 
                AND OCO.nome_bairro IN ('IPIRANGA', 'ALTINO BARBOSA', 'SÃO FRANCISCO', 'SAO FRANCISCO', 'TABAJARAS', 'GRÃO PARÁ', 'GRAO PARA', 'FÁTIMA', 'FATIMA', 'JARDIM IRACEMA', 'SANTA CLARA', 'VILA SÃO JOÃO', 'VILA SAO JOAO', 'TURMA 37', 'BELVEDERE', 'CASTRO PIRES', 'RESIDENCIAL GRAN VIVER', 'GRAN VIVER', 'FRIMUSA') THEN '2 PEL/47 CIA PM/19 BPM'
            WHEN OCO.nome_municipio = 'TEOFILO OTONI' 
                AND OCO.nome_bairro IN ('MANOEL PIMENTA', 'FREI JÚLIO', 'FREI JULIO', 'TEÓFILO ROCHA', 'TEOFILO ROCHA', 'FUNCIONÁRIOS', 'FUNCIONARIOS', 'MUCURI', 'LOURIVAL SOARES DA COSTA', 'ESPERANÇA', 'ESPERANCA', 'SOLIDARIEDADE', 'AEROPORTO', 'VILA BARREIROS', 'SÃO BENEDITO', 'SAO BENEDITO', 'SANTO ANTÔNIO', 'SANTO ANTONIO', 'JARDIM SÃO PAULO', 'JARDIM SAO PAULO', 'CIDADE NOVA') THEN '3 PEL/47 CIA PM/19 BPM'
            WHEN OCO.nome_municipio = 'TEOFILO OTONI' 
                AND OCO.nome_bairro IN ('SÃO CRISTÓVÃO', 'SAO CRISTOVAO', 'VIRIATO', 'JOAQUIM PEDROSA', 'GANGORRINHA', 'CONCÓRDIA', 'CONCORDIA', 'MINAS NOVA', 'FREI DIMAS', 'PALMEIRAS', 'SÃO DIOGO', 'SAO DIOGO', 'INDAIA', 'INDAIÁ', 'JARDIM DAS ACÁCIAS', 'JARDIM DAS ACACIAS') THEN '1 PEL/42 CIA PM/19 BPM'
            WHEN OCO.nome_municipio = 'TEOFILO OTONI' 
                AND OCO.nome_bairro IN ('ITAGUAÇU', 'ITAGUACU', 'BELA VISTA', 'VILA BETEL', 'FELICIDADE', 'JARDIM FLORESTA', 'JARDIM SERRA VERDE', 'MATINHA', 'MONTE CARLO', 'NOVO HORIZONTE', 'PAMPULHINHA', 'RESIDENCIAL', 'LARANJEIRAS', 'JARDIM FLORESTA', 'SÃO JACINTO', 'SAO JACINTO') THEN '2 PEL/42 CIA PM/19 BPM'
            WHEN OCO.nome_municipio = 'NOVO CRUZEIRO' THEN '1 PEL/232 CIA PM/19 BPM'
            WHEN OCO.nome_municipio IN ('CARAI', 'CARAÍ', 'CATUJI', 'ITAIPE', 'ITAIPÉ') THEN '2 PEL/232 CIA PM/19 BPM'
            WHEN OCO.nome_municipio IN ('FRANCISCOPOLIS', 'FRANCISCÓPOLIS', 'LADAINHA', 'MALACACHETA', 'POTE', 'POTÉ', 'SETUBINHA') THEN '3 PEL/232 CIA PM/19 BPM'
            WHEN OCO.nome_municipio IN ('CAMPANARIO', 'CAMPANÁRIO', 'FREI GASPAR', 'ITAMBACURI', 'JAMPRUCA', 'NOVA MODICA', 'NOVA MÓDICA', 'PESCADOR', 'SAO JOSE DO DIVINO', 'SÃO JOSÉ DO DIVINO') THEN '1 PEL/155 CIA PM/19 BPM'
            WHEN OCO.nome_municipio IN ('ATALEIA', 'ATALÉIA', 'OURO VERDE DE MINAS') THEN '2 PEL/155 CIA PM/19 BPM'
            WHEN OCO.nome_municipio IN ('NOVO ORIENTE DE MINAS', 'PAVAO', 'PAVÃO') THEN '3 PEL/42 CIA PM/19 BPM'
            ELSE '3 PEL/42 CIA PM/19 BPM'
        END AS PELOTAO_PM,
        OCO.data_hora_fato AS data_hora_fato,
        CAST(OCO.data_hora_fato AS DATE) AS data_fato,
        YEAR(OCO.data_hora_fato) AS ano,
        MONTH(OCO.data_hora_fato) AS mes,
        OCO.data_hora_fechamento ,
        OCO.data_hora_alteracao ,
        OCO.data_hora_primeiro_fim_preenchimento ,
        OCO.nome_tipo_relatorio ,
        OCO.digitador_cargo_efetivo ,
        OCO.digitador_matricula ,
        OCO.digitador_nome ,
        OCO.digitador_nome_unidade ,
        OCO.natureza_codigo ,
        OCO.natureza_descricao_longa ,
        OCO.historico_ocorrencia 
	FROM
		db_bisp_reds_reporting.tb_ocorrencia AS OCO
	WHERE 1=1
		AND (OCO.nome_tipo_relatorio IN ('POLICIAL', 'AMBIENTAL', 'TRANSITO') OR OCO.natureza_codigo IN ('W01000', 'W02000', 'W03000', 'W04000'))
		AND OCO.unidade_responsavel_registro_nome LIKE '%19 BPM%'
		AND YEAR(data_hora_fato) = 2026 
		AND MONTH(data_hora_fato)>= 1
		AND OCO.digitador_sigla_orgao = 'PM'
	
