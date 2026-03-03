WITH
	RAT_REDS AS (
	SELECT
		RAT.numero_ocorrencia,
		RAT.data_hora_fato,
		MONTH (RAT.data_hora_fato) as 'Mes',
		YEAR (RAT.data_hora_fato) as 'Ano',
		HOUR (RAT.data_hora_fato) as 'Hora',
		RAT.nome_tipo_relatorio,
		RAT.natureza_codigo,
		RAT.natureza_descricao,
		RAT.logradouro_nome,
		RAT.codigo_bairro,
		RAT.nome_bairro,
		RAT.codigo_municipio,
		CASE 
			WHEN RAT.numero_ocorrencia = '2025-005275459-001' THEN 'TEOFILO OTONI'
			ELSE RAT.nome_municipio
		END	AS nome_municipio,
		RAT.tipo_local_descricao,
		RAT.local_imediato_codigo,
		RAT.local_imediato_descricao,
		RAT.numero_latitude,
		RAT.numero_longitude,
		RAT.unidade_responsavel_registro_nome,
		RAT.nome_operacao,
		RAT.natureza_secundaria1_codigo,
		RAT.natureza_secundaria2_codigo,
		RAT.natureza_secundaria3_codigo,
		CASE
			WHEN RAT.codigo_municipio in  (310470,311080,311300,311545,312675,312680,313230,313270,313507,313700,313920,314490,314530,314535,314620,314850,315000,315240,316330,316555,316860) THEN '19 BPM'
			--WHEN RAT.codigo_municipio in (315780) THEN '35 BPM'
			--WHEN RAT.codigo_municipio in (316295, 317120) THEN '36 BPM'
			--WHEN RAT.codigo_municipio in (313760, 311787, 315900, 313460) THEN '8 CIA PM IND'
			--WHEN RAT.codigo_municipio in (312170, 313190, 314000, 314610) THEN '52 BPM'
			--WHEN RAT.codigo_municipio in (311000, 313660, 315670, 316830) THEN '61 BPM'
			ELSE 'OUTROS'
		END AS UEOP_2024,
		CASE
			WHEN (RAT.natureza_codigo IN ('Y04009', 'Y07001', 'Y07002', 'Y07003', 'Y07010', 'Y10001')
				OR RAT.natureza_secundaria1_codigo = 'Y10001'
	            OR RAT.natureza_secundaria2_codigo = 'Y10001'
	            OR RAT.natureza_secundaria3_codigo = 'Y10001') THEN 'POG'
			WHEN RAT.natureza_codigo IN ('Y15001', 'Y15010', 'Y07014') THEN 'PP'
			WHEN RAT.natureza_codigo IN ('Y07012', 'Y15020', 'Y15052') THEN 'ESPECIFICA'
			ELSE 'OUTROS'
		END AS TIPO_OPERACAO
		FROM
		db_bisp_reds_reporting.tb_ocorrencia RAT
	WHERE
		RAT.codigo_municipio in (310470,311080,311300,311545,312675,312680,313230,313270,313507,313700,313920,314490,314530,314535,314620,314850,315000,315240,316330,316555,316860)
		--AND RAT.nome_tipo_relatorio = 'RAT'
		AND YEAR(RAT.data_hora_fato) = 2026)
		--AND RAT.unidade_responsavel_registro_nome LIKE '%/19 BPM%'
	SELECT 
		RAT.*,
		CASE
			WHEN RAT.UEOP_2024 = '19 BPM'
			AND RAT.unidade_responsavel_registro_nome LIKE '%42 CIA PM%' THEN '42 CIA PM/19 BPM'
			WHEN RAT.UEOP_2024 = '19 BPM'
			AND RAT.unidade_responsavel_registro_nome LIKE '%47 CIA PM%' THEN '47 CIA PM/19 BPM'
			WHEN RAT.UEOP_2024 = '19 BPM'
			AND RAT.unidade_responsavel_registro_nome LIKE '%155 CIA PM%' THEN '155 CIA PM/19 BPM'
			WHEN RAT.UEOP_2024 = '19 BPM'
			AND RAT.unidade_responsavel_registro_nome LIKE '%232 CIA PM%' THEN '232 CIA PM/19 BPM'
			WHEN RAT.UEOP_2024 = '19 BPM'
			AND RAT.unidade_responsavel_registro_nome LIKE '%74 CIA TM%' THEN '74 CIA TM/19 BPM'
		END AS CIA_PM,
		 CASE
	        WHEN RAT.nome_municipio = 'TEOFILO OTONI' 
	             AND RAT.nome_bairro IN ('CENTRO', 'CIDADE ALTA', 'MARAJOARA', 'DR LAERTE LAENDER', 'OLGA PRATES CORREA', 'FILADÉLFIA', 'SÃO DIOGO', 'SAO DIOGO', 'FILADELFIA', 'DOUTOR LAERTE LAENDER') THEN '1 PEL/47 CIA PM/19 BPM'
	        WHEN RAT.nome_municipio = 'TEOFILO OTONI' 
	             AND RAT.nome_bairro IN ('IPIRANGA', 'ALTINO BARBOSA', 'SÃO FRANCISCO', 'SAO FRANCISCO', 'TABAJARAS', 'GRÃO PARÁ', 'GRAO PARA', 'FÁTIMA', 'FATIMA', 'JARDIM IRACEMA', 'SANTA CLARA', 'VILA SÃO JOÃO', 'VILA SAO JOAO', 'TURMA 37', 'BELVEDERE', 'CASTRO PIRES', 'RESIDENCIAL GRAN VIVER', 'GRAN VIVER', 'FRIMUSA') THEN '2 PEL/47 CIA PM/19 BPM'
	        WHEN RAT.nome_municipio = 'TEOFILO OTONI' 
	             AND RAT.nome_bairro IN ('MANOEL PIMENTA', 'FREI JÚLIO', 'FREI JULIO', 'TEÓFILO ROCHA', 'TEOFILO ROCHA', 'FUNCIONÁRIOS', 'FUNCIONARIOS', 'MUCURI', 'LOURIVAL SOARES DA COSTA', 'ESPERANÇA', 'ESPERANCA', 'SOLIDARIEDADE', 'AEROPORTO', 'VILA BARREIROS', 'SÃO BENEDITO', 'SAO BENEDITO', 'SANTO ANTÔNIO', 'SANTO ANTONIO', 'JARDIM SÃO PAULO', 'JARDIM SAO PAULO', 'CIDADE NOVA') THEN '3 PEL/47 CIA PM/19 BPM'
	        WHEN RAT.nome_municipio = 'TEOFILO OTONI' 
	             AND RAT.nome_bairro IN ('SÃO CRISTÓVÃO', 'SAO CRISTOVAO', 'VIRIATO', 'JOAQUIM PEDROSA', 'GANGORRINHA', 'CONCÓRDIA', 'CONCORDIA', 'MINAS NOVA', 'FREI DIMAS', 'PALMEIRAS', 'SÃO DIOGO', 'SAO DIOGO', 'INDAIA', 'INDAIÁ', 'JARDIM DAS ACÁCIAS', 'JARDIM DAS ACACIAS') THEN '1 PEL/42 CIA PM/19 BPM'
	        WHEN RAT.nome_municipio = 'TEOFILO OTONI' 
	             AND RAT.nome_bairro IN ('ITAGUAÇU', 'ITAGUACU', 'BELA VISTA', 'VILA BETEL', 'FELICIDADE', 'JARDIM FLORESTA', 'JARDIM SERRA VERDE', 'MATINHA', 'MONTE CARLO', 'NOVO HORIZONTE', 'PAMPULHINHA', 'RESIDENCIAL', 'LARANJEIRAS', 'JARDIM FLORESTA', 'SÃO JACINTO', 'SAO JACINTO') THEN '2 PEL/42 CIA PM/19 BPM'
	        WHEN RAT.nome_municipio = 'NOVO CRUZEIRO' THEN '1 PEL/232 CIA PM/19 BPM'
	        WHEN RAT.nome_municipio IN ('CARAI', 'CARAÍ', 'CATUJI', 'ITAIPE', 'ITAIPÉ') THEN '2 PEL/232 CIA PM/19 BPM'
	        WHEN RAT.nome_municipio IN ('FRANCISCOPOLIS', 'FRANCISCÓPOLIS', 'LADAINHA', 'MALACACHETA', 'POTE', 'POTÉ', 'SETUBINHA') THEN '3 PEL/232 CIA PM/19 BPM'
	        WHEN RAT.nome_municipio IN ('CAMPANARIO', 'CAMPANÁRIO', 'FREI GASPAR', 'ITAMBACURI', 'JAMPRUCA', 'NOVA MODICA', 'NOVA MÓDICA', 'PESCADOR', 'SAO JOSE DO DIVINO', 'SÃO JOSÉ DO DIVINO') THEN '1 PEL/155 CIA PM/19 BPM'
	        WHEN RAT.nome_municipio IN ('ATALEIA', 'ATALÉIA', 'OURO VERDE DE MINAS') THEN '2 PEL/155 CIA PM/19 BPM'
	        WHEN RAT.nome_municipio IN ('NOVO ORIENTE DE MINAS', 'PAVAO', 'PAVÃO') THEN '3 PEL/42 CIA PM/19 BPM'
	        ELSE '3 PEL/42 CIA PM/19 BPM'
	    END AS PELOTAO_PM
	FROM
		RAT_REDS RAT
	WHERE
		RAT.codigo_municipio in (310470,311080,311300,311545,312675,312680,313230,313270,313507,313700,313920,314490,314530,314535,314620,314850,315000,315240,316330,316555,316860)
		--AND RAT.Ano = :ANO
		AND RAT.Mes >= 1--:MESINICIAL
		AND RAT.Mes <= 12--:MESFINAL
		AND (RAT.natureza_codigo IN ('Y04009', 'Y07001', 'Y07002', 'Y07003', 'Y07010', 'Y10001', 'Y07012', 'Y15001', 'Y15010', 'Y15020', 'Y15052', 'Y07014')
		    OR RAT.natureza_secundaria1_codigo = 'Y10001'
            OR RAT.natureza_secundaria2_codigo = 'Y10001'
            OR RAT.natureza_secundaria3_codigo = 'Y10001')
		AND RAT.unidade_responsavel_registro_nome LIKE '%/19 BPM%'
		;