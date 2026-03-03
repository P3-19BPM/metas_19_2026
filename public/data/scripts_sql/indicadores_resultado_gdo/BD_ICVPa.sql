SELECT                                                                
	    OCO.numero_ocorrencia,                                            -- GDO_2025_CVPa_Versao_20-02-2025 Número único que identifica a ocorrência
	    ENV.envolvimento_codigo,                                         -- Código que identifica o tipo de envolvimento na ocorrência
	    ENV.envolvimento_descricao,                                      -- Descrição do tipo de envolvimento na ocorrência
	    ENV.numero_envolvido,                                            -- Número único que identifica o envolvido
	    ENV.nome_completo_envolvido,                                     -- Nome completo do envolvido na ocorrência
	    ENV.nome_mae,                                                    -- Nome da mãe do envolvido
	    ENV.data_nascimento,                                            -- Data de nascimento do envolvido
	    ENV.condicao_fisica_descricao,                                  -- Descrição da condição física do envolvido
	   	CASE 
	   		WHEN ENV.numero_ocorrencia IN ('2025-007163020-001', '2025-002170326-001', '2025-010444621-001', '2025-022085982-001') THEN 'C01158'
	   		ELSE ENV.natureza_ocorrencia_codigo
	   	END	AS natureza_ocorrencia_codigo,                                 -- Código da natureza da ocorrência
	    ENV.natureza_ocorrencia_descricao,                              -- Descrição da natureza da ocorrência
	    ENV.ind_consumado,                                              -- Indicador se a ocorrência foi consumada (S) ou tentada (N)
		CASE 
	    	WHEN OCO.codigo_municipio IN (310090 , 310100 , 310170 , 310270 , 310340 , 310470 , 310520 , 310660 , 311080 , 311300 , 311370 , 311545 , 311700 , 311950 , 312015 , 312235 , 312245 , 312560 , 312675 , 312680 , 312705 , 313230 , 313270 , 313330 , 313400 , 313470 , 313507 , 313580 , 313600 , 313650 , 313700 , 313890 , 313920 , 314055 , 314140 , 314315 , 314430 , 314490 , 314530 , 314535 , 314620 , 314630 , 314675 , 314850 , 314870 , 315000 , 315217 , 315240 , 315510 , 315660 , 315710 , 315765 , 315810 , 316030 , 316330 , 316555 , 316670 , 316860 , 317030 , 317160) THEN '15 RPM'	
		    ELSE 'OUTROS'	
	   	END AS RPM_2024,
		CASE 
			WHEN OCO.codigo_municipio IN (310470,311080,311300,311545,312675,312680,313230,313270,313507,313700,313920,314490,314530,314535,314620,314850,315000,315240,316330,316555,316860) THEN '19 BPM'
			ELSE 'OUTROS' 
		END AS UEOP_2024,	
	    OCO.unidade_area_militar_codigo,                                -- Código da unidade militar responsável pela área
	    OCO.unidade_area_militar_nome,                                  -- Nome da unidade militar responsável pela área
	    OCO.unidade_responsavel_registro_codigo,                        -- Código da unidade que registrou a ocorrência
	    OCO.unidade_responsavel_registro_nome,                          -- Nome da unidade que registrou a ocorrência
	    CAST(OCO.codigo_municipio AS INTEGER),                          -- Converte o código do município para número inteiro
	    CASE 
		    WHEN ENV.numero_ocorrencia IN ('2025-010444621-001') THEN 'POTE' 
		    WHEN ENV.numero_ocorrencia IN ('2025-022085982-001') THEN 'TEOFILO OTONI'
	   		WHEN ENV.numero_ocorrencia IN ('2025-014842395-001') THEN 'CATUJI'  --ALTERA O NOME DO MUNICIPIO DA OCORRENCIA 2025-010444621-001
	   		ELSE OCO.nome_municipio
	   	END	AS nome_municipio,                                            -- Nome do município onde ocorreu o fato
	    OCO.tipo_logradouro_descricao,                                -- Tipo do logradouro (Rua, Avenida, etc)
	    OCO.logradouro_nome,                                          -- Nome do logradouro
	    OCO.numero_endereco,                                          -- Número do endereço
	    OCO.nome_bairro,                                             -- Nome do bairro
	    OCO.ocorrencia_uf,                                           -- UF onde ocorreu o fato
	    CASE 
		    WHEN ENV.numero_ocorrencia IN ('2025-010444621-001') THEN -17.8157877
		    WHEN ENV.numero_ocorrencia IN ('2025-022085982-001') THEN -17.8892724
	   		WHEN ENV.numero_ocorrencia IN ('2025-014842395-001') THEN -17.40172254146  --ALTERA A LATITUDE DA OCORRENCIA
	   		ELSE OCO.numero_latitude
	   	END	AS numero_latitude,                                         -- Latitude do local da ocorrência
	    CASE 
		    WHEN ENV.numero_ocorrencia IN ('2025-010444621-001') THEN -41.7594857
		    WHEN ENV.numero_ocorrencia IN ('2025-022085982-001') THEN -41.5091289
	   		WHEN ENV.numero_ocorrencia IN ('2025-014842395-001') THEN -41.5194589866  --ALTERA A LONGITUDE DA OCORRENCIA
	   		ELSE OCO.numero_longitude
	   	END	AS numero_longitude,                                        -- Longitude do local da ocorrência
	    OCO.data_hora_fato,                                         -- Data e hora em que ocorreu o fato
	    YEAR(OCO.data_hora_fato) AS ano,                            -- Extrai o ano da data do fato
	    MONTH(OCO.data_hora_fato) AS mes,                           -- Extrai o mês da data do fato
	    OCO.nome_tipo_relatorio,                                    -- Tipo do relatório (POLICIAL ou REFAP)
	    OCO.digitador_sigla_orgao                                   -- Sigla do órgão que registrou (PM ou PC)
	FROM db_bisp_reds_reporting.tb_ocorrencia AS OCO                    -- Tabela principal de ocorrências
	INNER JOIN db_bisp_reds_reporting.tb_envolvido_ocorrencia AS ENV    -- Join com a tabela de envolvidos
	    ON OCO.numero_ocorrencia = ENV.numero_ocorrencia                -- Relaciona ocorrências com seus envolvidos
	WHERE 1=1                                                          
	    AND (
			(OCO.numero_ocorrencia = '2026-003307176-001' AND ENV.numero_envolvido = 2)
			OR (OCO.numero_ocorrencia = '2025-032221274-001' AND ENV.numero_envolvido = 1)
			OR (
				OCO.numero_ocorrencia NOT IN ('2026-003307176-001', '2025-032221274-001')
				AND ENV.id_envolvimento IN (25,32,1097,26,27,28,872)
			)
		) -- Filtra tipos específicos de envolvimento (Todos vitima)
	    AND (ENV.natureza_ocorrencia_codigo IN ('C01157','C01158','C01159') OR ENV.numero_ocorrencia IN ('2025-007163020-001', '2025-002170326-001', '2025-010444621-001') OR OCO.numero_ocorrencia IN ('2025-032221274-001') ) -- Filtra naturezas específicas das ocorrências (Roubo,Extorsão,Extorsão Mediante Sequestro)
	    AND ENV.condicao_fisica_codigo IS DISTINCT FROM '0100'        -- Exclui condição física específica (FATAL)
	    AND ENV.ind_consumado IN ('S','N')                             -- Filtra ocorrências consumadas e tentadas
	    AND OCO.ocorrencia_uf = 'MG'                                   -- Filtra apenas ocorrências de Minas Gerais
	    AND OCO.digitador_sigla_orgao IN ('PM','PC')                   -- Filtra registros feitos pela PM ou PC
	    AND OCO.nome_tipo_relatorio IN ('POLICIAL','REFAP')            -- Filtra tipos específicos de relatório (POLICIAL ou REFAP)
	    AND YEAR(OCO.data_hora_fato) >= 2026--:ANO                            -- Filtra pelo ano informado no parâmetro
	    AND MONTH(OCO.data_hora_fato) >= 1--:MESINICIAL                   -- Filtra a partir do mês inicial informado
	    AND MONTH(OCO.data_hora_fato) <= 12--:MESFINAL                     -- Filtra até o mês final informado
	    AND OCO.ind_estado = 'F'                                       -- Filtra apenas ocorrências finalizadas
	    AND OCO.numero_ocorrencia NOT IN ( '2025-017413597-001', '2025-017123929-001','2025-005944164-001', '2025-026017547-001', '2025-025659417-001', '2025-039435479-001', '2025-036713056-001', '2025-041706240-001', '2025-042114519-001', '2025-047512998-001', '2025-052643614-001', '2026-000693462-001')
	    AND OCO.codigo_municipio in (310470,311080,311300,311545,312675,312680,313230,313270,313507,313700,313920,314490,314530,314535,314620,314850,315000,315240,316330,316555,316860)
		-- NOVA CONDIÇÃO PARA FILTRAR O ENVOLVIDO ESPECÍFICO NA OCORRÊNCIA DESEJADA
    	AND (OCO.numero_ocorrencia <> '2025-044360359-001' OR ENV.numero_envolvido = 2)
		 -- PARA RESGATAR APENAS OS DADOS DOS MUNICÍPIOS SOB SUA RESPONSABILIDADE, REMOVA O COMENTÁRIO E ADICIONE O CÓDIGO DE MUNICIPIO DA SUA RESPONSABILIDADE. NO INÍCIO DO SCRIPT, É POSSÍVEL VERIFICAR ESSES CÓDIGOS, POR RPM E UEOP.
	   -- AND OCO.unidade_area_militar_nome LIKE '%x BPM/x RPM%' -- Filtra pelo nome da unidade área militar
	ORDER BY                                                           -- Ordenação dos resultados
	    RPM_2024,                                                     -- Primeiro por RPM
	    UEOP_2024,                                                    -- Depois por Batalhão
	    OCO.data_hora_fato,                                          -- Depois por data/hora
	    OCO.numero_ocorrencia,                                       -- Depois por número da ocorrência
	    ENV.nome_completo_envolvido,                                 -- Depois por nome do envolvido
	    ENV.nome_mae,                                                -- Depois por nome da mãe
	    ENV.data_nascimento;                                         