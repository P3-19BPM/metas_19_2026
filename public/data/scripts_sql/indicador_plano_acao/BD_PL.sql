SELECT 
	    T.TIPO,
	    T.cia_pm,
	    T.PELOTAO_PM,
	    T.numero_ocorrencia,
	    T.natureza_codigo,
	    T.natureza_descricao_longa,
	    T.nome_municipio,
	    T.nome_bairro,
	    T.data_hora_fato,
	    CASE
	        WHEN T.TIPO IN ('PVD', 'POS DELITO') THEN
	            CASE
	                WHEN T.natureza_codigo LIKE 'B01121' THEN 'B01121'
	                WHEN T.natureza_codigo LIKE 'B01129' THEN 'B01129'
	                WHEN T.natureza_codigo LIKE 'B01147' THEN 'B01147'
	                ELSE
	                    CASE
	                        WHEN T.natureza_secundaria1_codigo LIKE 'B01121' THEN 'B01121'
	                        WHEN T.natureza_secundaria1_codigo LIKE 'B01129' THEN 'B01129'
	                        WHEN T.natureza_secundaria1_codigo LIKE 'B01147' THEN 'B01147'
	                        ELSE
	                            CASE
	                                WHEN T.natureza_secundaria2_codigo LIKE 'B01121' THEN 'B01121'
	                                WHEN T.natureza_secundaria2_codigo LIKE 'B01129' THEN 'B01129'
	                                WHEN T.natureza_secundaria2_codigo LIKE 'B01147' THEN 'B01147'
	                                ELSE
	                                    CASE
	                                        WHEN T.natureza_secundaria3_codigo LIKE 'B01121' THEN 'B01121'
	                                        WHEN T.natureza_secundaria3_codigo LIKE 'B01129' THEN 'B01129'
	                                        WHEN T.natureza_secundaria3_codigo LIKE 'B01147' THEN 'B01147'
	                                        ELSE ''
	                                    END
	                            END
	                    END
	            END
	        ELSE ''
	    END AS NATUREZA_POS,
	    T.nome_operacao,
	    T.digitador_matricula,
	    T.digitador_nome,
	    T.numero_latitude,
	    T.numero_longitude
	    --T.*
	FROM (
	    SELECT 
	        CASE -- Criando uma coluna de TIPO com base em cada Item do Plano de Ação, pra facilitar a filtragem no monitoramento
	            WHEN OCO.ind_violencia_domestica = 'S' 
	                AND (OCO.natureza_codigo IN ('B01121', 'B01129', 'B01147')
	                OR OCO.natureza_secundaria1_codigo IN ('B01121', 'B01129', 'B01147')
	                OR OCO.natureza_secundaria2_codigo IN ('B01121', 'B01129', 'B01147')
	                OR OCO.natureza_secundaria3_codigo IN ('B01121', 'B01129', 'B01147')) THEN 'PVD'
	            WHEN OCO.ind_violencia_domestica = 'N' 
	                AND (OCO.natureza_codigo IN ('B01121', 'B01129', 'B01147')
	                OR OCO.natureza_secundaria1_codigo IN ('B01121', 'B01129', 'B01147')
	                OR OCO.natureza_secundaria2_codigo IN ('B01121', 'B01129', 'B01147')
	                OR OCO.natureza_secundaria3_codigo IN ('B01121', 'B01129', 'B01147')) THEN 'POS DELITO'
	             WHEN (OCO.natureza_codigo IN ('I04033')
	                OR OCO.natureza_secundaria1_codigo IN ('I04033')
	                OR OCO.natureza_secundaria2_codigo IN ('I04033')
	                OR OCO.natureza_secundaria3_codigo IN ('I04033')) THEN 'IDTG'
	            WHEN OCO.natureza_codigo = 'A23001' THEN 'EGRESSO_CUMPRINDO'
	            --WHEN OCO.natureza_codigo = 'X01000' THEN 'SUPERVISAO'
	            WHEN OCO.natureza_codigo = 'A23002' THEN 'EGRESSO_DESCUMPRINDO'
	            --WHEN OCO.natureza_codigo = 'Y15001' THEN 'ESCOLAR'
	            WHEN OCO.natureza_codigo = 'Y07005' THEN 'BOEMIA'
	            WHEN (OCO.natureza_codigo = 'Y15099' 
	            		AND (	OCO.nome_operacao LIKE '%BOEMIA%' 
	            				OR OCO.nome_operacao LIKE '%BOÊMIA%'
	            				OR OCO.nome_operacao LIKE '%Boemia%'
	            				OR OCO.nome_operacao LIKE '%EMIA%'
	            				OR OCO.nome_operacao LIKE '%êmia%'
	            				OR OCO.nome_operacao LIKE '%ÊMIA%'
	            				OR OCO.nome_operacao LIKE '%emia%'
	            				OR OCO.nome_operacao LIKE '%boemia%'
	            				OR OCO.nome_operacao LIKE '%Boêmia%'
	            				OR OCO.nome_operacao LIKE '%boêmia%')) THEN 'BOEMIA'
	           	WHEN (OCO.natureza_codigo = 'Y15099' 
	            		AND (	OCO.nome_operacao LIKE '%SEGURA%' 
	            				OR OCO.nome_operacao LIKE '%segura%'
	            				OR OCO.nome_operacao LIKE '%Segura%')) THEN 'ESCOLAR'
	            WHEN (OCO.natureza_codigo = 'Y15099' 
	            		AND (	OCO.nome_operacao LIKE '%CAMPO%' 
	            				OR OCO.nome_operacao LIKE '%Campo%'
	            				OR OCO.nome_operacao LIKE '%rural%'
	            				OR OCO.nome_operacao LIKE '%RURAL%'
	            				OR OCO.nome_operacao LIKE '%campo%')) THEN 'RURAL'
	            WHEN OCO.natureza_codigo = 'Y15010' THEN 'RURAL'
	            WHEN OCO.natureza_codigo = 'Y15001' THEN 'ESCOLAR'
	            WHEN OCO.natureza_codigo = 'A20002' THEN 'A20002'
	            WHEN OCO.natureza_codigo = 'A20000' THEN 'A20000'
	            WHEN OCO.natureza_codigo = 'A20001' THEN 'A20000'
	            WHEN OCO.natureza_codigo = 'A23000' THEN 'A23000'
	            WHEN OCO.natureza_codigo IN ('Y04011', 'Y01003', 'Y04009') THEN 'CAVALO_ACO'
	           	WHEN OCO.data_hora_fato <= '2024-08-12' AND OCO.natureza_codigo = 'Y15099' THEN 'BOEMIA'
	           	WHEN OCO.natureza_codigo = 'Y15099' THEN 'Y15099_SEM_CONTAR'
	            WHEN OCO.nome_tipo_relatorio = 'RAT' THEN 'RAT'
	            ELSE 'OUTRO'
	        END AS TIPO,
	        CASE-- Criando coluna de CIA PM
	            WHEN OCO.unidade_area_militar_nome IS NOT NULL THEN 
	                CASE
		                WHEN (OCO.unidade_responsavel_registro_nome LIKE '%74 CIA TM%' AND OCO.natureza_codigo IN ('A23001', 'A23002', 'Y07001', 'Y07003', 'Y01003', 'Y15099', 'Y04012')) THEN '74 CIA TM'
	                    WHEN OCO.unidade_area_militar_nome LIKE '%42 CIA PM%' THEN '42 CIA PM' 
	                    WHEN OCO.unidade_area_militar_nome LIKE '%47 CIA PM%' THEN '47 CIA PM' 
	                    WHEN OCO.unidade_area_militar_nome LIKE '%155 CIA PM%' THEN '155 CIA PM' 
	                    WHEN OCO.unidade_area_militar_nome LIKE '%232 CIA PM%' THEN '232 CIA PM'                    
	                    ELSE ''
	                END 
	            ELSE 
	                CASE 
		                WHEN OCO.PELOTAO_PM LIKE '%74 CIA TM%' THEN '74 CIA TM'
	                    WHEN OCO.PELOTAO_PM LIKE '%42 CIA PM%' THEN '42 CIA PM' 
	                    WHEN OCO.PELOTAO_PM LIKE '%47 CIA PM%' THEN '47 CIA PM' 
	                    WHEN OCO.PELOTAO_PM LIKE '%155 CIA PM%' THEN '155 CIA PM' 
	                    WHEN OCO.PELOTAO_PM LIKE '%232 CIA PM%' THEN '232 CIA PM'
	                    ELSE ''
	                END 
	        END AS cia_pm,
	        OCO.*
	    FROM (
	    SELECT --Criando uma coluna de Pelotão. Nos Municipios em que temos mais de um pelotão atuante, utilizamos os nomes dos bairros para definir o Pelotão, nos demais, através do Municipio.
	            CASE
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
	            OCO.unidade_area_militar_nome,
	            OCO.unidade_responsavel_registro_nome,
	            OCO.numero_ocorrencia,
	            OCO.natureza_codigo,
	            OCO.natureza_descricao_longa,
	            OCO.nome_municipio,
	            OCO.nome_bairro,
	            OCO.ind_violencia_domestica,
	            OCO.natureza_secundaria1_codigo,
	            OCO.natureza_secundaria2_codigo,
	            OCO.natureza_secundaria3_codigo,
	            OCO.nome_tipo_relatorio,
	            OCO.natureza_ind_consumado,
	            OCO.nome_operacao,
	            OCO.numero_latitude,
	            OCO.numero_longitude,
				OCO.historico_ocorrencia ,
				OCO.data_hora_fato,
				OCO.digitador_matricula,
				OCO.digitador_nome
	        FROM 
	            db_bisp_reds_reporting.tb_ocorrencia as OCO
	        WHERE 1=1
	            AND OCO.codigo_municipio IN (310470,311080,311300,311545,312675,312680,313230,313270,313507,313700,313920,314490,314530,314535,314620,314850,315000,315240,316330,316555,316860)
	            AND ((OCO.natureza_codigo IN (	'A20000', 'A20002','A23001', 'A23002','Y01003','Y15099', 'Y04011', 'A20001',
	            								 'Y07005', 'Y07001', 'Y07003', 'Y15010', 'Y15001', 'Y04009') AND OCO.unidade_responsavel_registro_nome LIKE '%19 BPM%')
	                OR (OCO.natureza_codigo IN ('B01121') AND OCO.natureza_ind_consumado = 'N')
	                OR OCO.natureza_codigo IN ( 'B01129', 'B01147')
	                OR OCO.natureza_secundaria1_codigo IN ('B01121', 'B01129', 'B01147', 'A23002', 'A23002')
	                OR OCO.natureza_secundaria2_codigo IN ('B01121', 'B01129', 'B01147', 'A23002', 'A23002')
	                OR OCO.natureza_secundaria3_codigo IN ('B01121', 'B01129', 'B01147', 'A23002', 'A23002')
	            )
	            AND YEAR(OCO.data_hora_fato) >= 2026
	            AND MONTH (OCO.data_hora_fato) >= 1
	            AND MONTH (OCO.data_hora_fato) <= 12
	    ) AS OCO
	) AS T;