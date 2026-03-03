WITH                                                                    -- Início da definição da Common Table Expression (CTE)
	LETALIDADE AS                                                              -- Define uma CTE chamada LETALIDADE que será usada para filtrar ocorrências
	( 
	    SELECT                                                                 
	        ENV.numero_ocorrencia,                                            -- Seleciona o número da ocorrência da tabela de envolvidos
	        ENV.digitador_id_orgao,                                          -- Seleciona o ID do órgão que registrou a ocorrência
	        ENV.natureza_ocorrencia_codigo,                                  -- Seleciona o código da natureza da ocorrência
	        ENV.data_hora_fato,                                             -- Seleciona a data e hora do fato
	        ENV.ind_militar_policial_servico                                -- Seleciona o indicador se o militar estava em serviço
	    FROM 
	        db_bisp_reds_reporting.tb_envolvido_ocorrencia ENV              -- Tabela origem dos dados de envolvidos
	    WHERE 1=1                                                           -- Início das condições de filtro (1=1 é sempre verdadeiro)
	        AND ENV.natureza_ocorrencia_codigo IN('B01121','B01129')  -- Filtra natureza específica (Homicídio, Lesão Corporal)
	        AND ENV.id_envolvimento IN (35,36,44)                          -- Filtra apenas autores, co-autores e suspeitos
	        AND ENV.ind_militar_policial IS NOT DISTINCT FROM 'M'          -- Filtra apenas militares
	        AND ENV.ind_militar_policial_servico IS NOT DISTINCT FROM 'S'  -- Filtra apenas militares em serviço
	        AND YEAR(ENV.data_hora_fato) >= 2016 -- :ANO                           -- Filtra pelo ano informado no parâmetro
	        AND MONTH(ENV.data_hora_fato) >= 1 --:MESINICIAL                  -- Filtra pelo mês inicial informado no parâmetro
	        AND MONTH(ENV.data_hora_fato) <= 12 --:MESFINAL                    -- Filtra pelo mês final informado no parâmetro
	)
	SELECT                                                           
	    OCO.numero_ocorrencia,                                           -- Seleciona o número da ocorrência
	    ENV.envolvimento_codigo,                                         -- Seleciona o código do tipo de envolvimento
	    ENV.envolvimento_descricao,                                     -- Seleciona a descrição do tipo de envolvimento
	    ENV.numero_envolvido,                                           -- Seleciona o número do envolvido
	CONCAT(                                                                    -- Início da concatenação principal que formará a chave do envolvido
	(CASE                                                                 -- Início do primeiro CASE para tratamento do nome do envolvido
	    WHEN UPPER(ENV.nome_completo_envolvido) IS NULL                      -- Verifica se o nome do envolvido é nulo
		    OR TRIM(UPPER(ENV.nome_completo_envolvido)) = ''                     -- Verifica se o nome do envolvido está vazio após remoção de espaços
		    OR UPPER(ENV.nome_completo_envolvido) LIKE '%DESCONHECID%'          -- Verifica se o nome contém variações de "DESCONHECIDO"
		    OR UPPER(ENV.nome_completo_envolvido) LIKE '%IGNORAD%'              -- Verifica se o nome contém variações de "IGNORADO"
		    OR UPPER(ENV.nome_completo_envolvido) LIKE '%IDENTIFICA%' THEN     -- Verifica se o nome contém variações de "IDENTIFICADO"
	    CONCAT('DESCONHECIDO ',                                             -- Início da concatenação para casos de nome inválido
		    CAST(SUBSTRING(ENV.numero_ocorrencia, 6,9) AS STRING),              -- Extrai e converte para string os dígitos 6-9 do número da ocorrência
		    '0',                                                                -- Adiciona um zero como separador
		    CAST(ENV.numero_envolvido AS STRING)                                -- Converte o número do envolvido para string
	    )
	    ELSE UPPER(ENV.nome_completo_envolvido)                            -- Se nome válido, utiliza o nome em maiúsculas
	END),
	    '-',                                                               -- Adiciona hífen como separador entre os campos da chave
	(CASE                                                              -- Início do segundo CASE para tratamento do nome da mãe
	    WHEN UPPER(ENV.nome_mae) IS NULL                                  -- Verifica se o nome da mãe é nulo
		    OR TRIM(UPPER(ENV.nome_mae)) = ''                                 -- Verifica se o nome da mãe está vazio após remoção de espaços
		    OR UPPER(ENV.nome_mae) LIKE '%DESCONHECID%'                      -- Verifica se o nome da mãe contém variações de "DESCONHECIDO"
		    OR UPPER(ENV.nome_completo_envolvido) LIKE '%IGNORAD%'           -- Verifica se o nome contém variações de "IGNORADO"
		    OR UPPER(ENV.nome_completo_envolvido) LIKE '%IDENTIFICA%'       -- Verifica se o nome contém variações de "IDENTIFICADO"
	    THEN
	    CONCAT(' MAE ',                                                  -- Início da concatenação para casos de nome da mãe inválido
		    CAST(SUBSTRING(ENV.numero_ocorrencia, 6,9) AS STRING),          -- Extrai e converte para string os dígitos 6-9 do número da ocorrência
		    '0',                                                            -- Adiciona um zero como separador
		    CAST(ENV.numero_envolvido AS STRING)                            -- Converte o número do envolvido para string
	    )
	    ELSE UPPER(ENV.nome_mae)                                        -- Se nome da mãe válido, utiliza o nome em maiúsculas
	 END),
	    '-',                                                           -- Adiciona hífen como separador entre os campos da chave
	(CASE                                                          -- Início do terceiro CASE para tratamento da data de nascimento
	    WHEN COALESCE(CAST(ENV.data_nascimento AS STRING), '') = ''   -- Verifica se a data de nascimento é nula ou vazia após conversão
	    THEN
		    CONCAT('CAMPO NULO ',                                         -- Início da concatenação para casos de data de nascimento inválida
		    CAST(SUBSTRING(ENV.numero_ocorrencia, 6,9) AS STRING),       -- Extrai e converte para string os dígitos 6-9 do número da ocorrência
		    '0',                                                         -- Adiciona um zero como separador
		    CAST(ENV.numero_envolvido AS STRING)                         -- Converte o número do envolvido para string
	    )
	    ELSE CAST(ENV.data_nascimento AS STRING)                     -- Se data válida, converte para string
	END)
	) AS chave_envolvido,                                             -- Nomeia o resultado da concatenação como "chave_envolvido"
	CASE                                                                              -- Inicia estrutura de decisão condicional
		WHEN UPPER(ENV.nome_completo_envolvido) IS NULL OR TRIM(UPPER(ENV.nome_completo_envolvido)) = ''  
			OR UPPER(ENV.nome_completo_envolvido) LIKE '%DESCONHECID%' 
			OR UPPER(ENV.nome_completo_envolvido) LIKE '%IGNORAD%' 
			OR UPPER(ENV.nome_completo_envolvido) LIKE '%IDENTIFICAD%'  
		THEN -- Verifica se nome está nulo, vazio ou contém 'DESCONHECID'
				CONCAT('DESCONHECIDO ',                                                          -- Inicia concatenação para criar identificador
				    CAST(SUBSTRING(ENV.numero_ocorrencia, 6,9) AS STRING),                      -- Extrai e converte dígitos 6-9 do número da ocorrência
				    '0',                                                                         -- Adiciona um zero como separador
				    CAST(ENV.numero_envolvido AS STRING)                                        -- Adiciona número do envolvido convertido para string
				)
		ELSE UPPER(ENV.nome_completo_envolvido)                                                -- Se nome existir, mantém o original
		END AS nome_completo_envolvido,-- Finaliza o CASE e nomeia o campo como nome_completo  
	CASE                                                                              -- Inicia estrutura de decisão condicional
			WHEN UPPER(ENV.nome_mae) IS NULL OR TRIM(UPPER(ENV.nome_mae)) = ''  
				OR UPPER(ENV.nome_mae) LIKE '%DESCONHECID%' 
				OR UPPER(ENV.nome_completo_envolvido) LIKE '%IGNORAD%' 
				OR UPPER(ENV.nome_completo_envolvido) LIKE '%IDENTIFICA%'  
			THEN                 -- Verifica se nome está nulo, vazio ou contém 'DESCONHECID'
					CONCAT('MÃE ',                                                          -- Inicia concatenação para criar identificador
					    CAST(SUBSTRING(ENV.numero_ocorrencia, 6,9) AS STRING),                      -- Extrai e converte dígitos 6-9 do número da ocorrência
					    '0',                                                                         -- Adiciona um zero como separador
					    CAST(ENV.numero_envolvido AS STRING)                                        -- Adiciona número do envolvido convertido para string
					)
			ELSE UPPER(ENV.nome_mae)                                                -- Se nome existir, mantém o original
	END AS nome_mae,                                                            -- Finaliza o CASE e nomeia o campo como nome_completo  
	CASE                                                                             -- Inicia estrutura de decisão condicional 
		WHEN COALESCE(CAST(ENV.data_nascimento AS STRING), '') = '' THEN                -- Verifica se o campo data_nascimento está vazio ou nulo usando COALESCE para tratar NULL como string vazia
			CONCAT(                                                                          -- Inicia concatenação para criar identificador
			   'CAMPO NULO ',                                                              -- String literal indicando campo nulo
			   CAST(SUBSTRING(ENV.numero_ocorrencia, 6,9) AS STRING),                      -- Extrai e converte dígitos 6-9 do número da ocorrência
			   '0',                                                                        -- Adiciona zero como separador
			   CAST(ENV.numero_envolvido AS STRING)                                        -- Adiciona número do envolvido convertido para string
			)
		ELSE CAST(ENV.data_nascimento AS STRING)                                        -- Se data existir, converte para string e retorna
	END AS data_nascimento,                                                         -- Finaliza o CASE e nomeia o campo como data_nascimento
	LET.ind_militar_policial_servico,                             -- Seleciona o indicador de militar em serviço(CTE)
	    ENV.condicao_fisica_descricao,                                -- Seleciona a descrição da condição física do envolvido
	    ENV.natureza_ocorrencia_codigo,                               -- Seleciona o código da natureza da ocorrência (envolvido)
	    ENV.natureza_ocorrencia_descricao,                           -- Seleciona a descrição da natureza da ocorrência (envolvido)
	    ENV.ind_consumado,                                           -- Seleciona se a ocorrência foi consumada ou tentada
		CASE 
	    	WHEN OCO.codigo_municipio IN (310090 , 310100 , 310170 , 310270 , 310340 , 310470 , 310520 , 310660 , 311080 , 311300 , 311370 , 311545 , 311700 , 311950 , 312015 , 312235 , 312245 , 312560 , 312675 , 312680 , 312705 , 313230 , 313270 , 313330 , 313400 , 313470 , 313507 , 313580 , 313600 , 313650 , 313700 , 313890 , 313920 , 314055 , 314140 , 314315 , 314430 , 314490 , 314530 , 314535 , 314620 , 314630 , 314675 , 314850 , 314870 , 315000 , 315217 , 315240 , 315510 , 315660 , 315710 , 315765 , 315810 , 316030 , 316330 , 316555 , 316670 , 316860 , 317030 , 317160) THEN '15 RPM'	
		    ELSE 'OUTROS'	
	   	END AS RPM_2024,
		CASE 
			WHEN OCO.codigo_municipio IN (310470,311080,311300,311545,312675,312680,313230,313270,313507,313700,313920,314490,314530,314535,314620,314850,315000,315240,316330,316555,316860) THEN '19 BPM'
			ELSE 'OUTROS' 
		END AS UEOP_2024,	
	    OCO.unidade_area_militar_codigo,                               -- Seleciona o código da unidade militar da área
	    OCO.unidade_area_militar_nome,                                -- Seleciona o nome da unidade militar da área
	    OCO.unidade_responsavel_registro_codigo,                      -- Seleciona o código da unidade responsável pelo registro
	    OCO.unidade_responsavel_registro_nome,                        -- Seleciona o nome da unidade responsável pelo registro
	    CAST(OCO.codigo_municipio AS INTEGER),                        -- Converte e seleciona o código do município
	    OCO.nome_municipio,                                          -- Seleciona o nome do município
	    OCO.tipo_logradouro_descricao,                              -- Seleciona o tipo do logradouro
	    OCO.logradouro_nome,                                        -- Seleciona o nome do logradouro
	    OCO.numero_endereco,                                        -- Seleciona o número do endereço
	    OCO.nome_bairro,                                           -- Seleciona o nome do bairro
	    OCO.ocorrencia_uf,                                         -- Seleciona a UF da ocorrência
	    OCO.numero_latitude,                                       -- Seleciona a latitude da ocorrência
	    OCO.numero_longitude,                                      -- Seleciona a longitude da ocorrência
	    OCO.data_hora_fato,                                       -- Seleciona a data e hora do fato
	    YEAR(OCO.data_hora_fato) AS ano,                          -- Extrai o ano da data do fato
	    MONTH(OCO.data_hora_fato) AS mes,                         -- Extrai o mês da data do fato
	    OCO.nome_tipo_relatorio,                                  -- Seleciona o tipo do relatório
	    OCO.digitador_sigla_orgao                                 -- Seleciona a sigla do órgão que registrou
	FROM db_bisp_reds_reporting.tb_ocorrencia AS OCO                -- Tabela principal de ocorrências
	INNER JOIN db_bisp_reds_reporting.tb_envolvido_ocorrencia AS ENV -- Join com a tabela de envolvidos
	    ON OCO.numero_ocorrencia = ENV.numero_ocorrencia 
	LEFT JOIN LETALIDADE LET                                         -- Join com a CTE de LETALIDADE
	    ON OCO.numero_ocorrencia = LET.numero_ocorrencia 
	WHERE 1=1                                                        
	    AND LET.numero_ocorrencia IS NULL                           -- Exclui ocorrências presentes na CTE LETALIDADE
	    AND ENV.id_envolvimento IN (25,32,1097,26,27,28,872)       -- Filtra tipos específicos de envolvimento (Todos as vitimas)
	    AND ENV.natureza_ocorrencia_codigo IN ('B01121','C01157','B02001','B01129','B01148','B01504')-- Filtra naturezas específicas (Homicídio,Roubo,Tortura,Lesão corporal,Sequestro e cárcere privado, Feminicídio* )
	    AND (ENV.condicao_fisica_codigo = '0100'    OR oco.numero_ocorrencia =    '2025-025232870-001'          )    -- Filtra por condição física específica (Fatal)
	    AND OCO.ocorrencia_uf = 'MG'                               -- Filtra apenas ocorrências de Minas Gerais
	    AND OCO.digitador_sigla_orgao IN ('PM','PC')               -- Filtra registros feitos pela PM ou PC
	    AND OCO.nome_tipo_relatorio IN ('POLICIAL','REFAP')        -- Filtra tipos específicos de relatório (POLICIAL e REFAP)
	    AND YEAR(OCO.data_hora_fato) >= 2026--:ANO                        -- Filtra pelo ano informado
	    AND MONTH(OCO.data_hora_fato) >= 1 --:MESINICIAL               -- Filtra pelo mês inicial
	    AND MONTH(OCO.data_hora_fato) <= 12 --:MESFINAL                 -- Filtra pelo mês final
	    AND (OCO.ind_estado IN ('F') OR oco.numero_ocorrencia IN ('2025-038410346-001', '2025-040408923-001'))
	    AND oco.numero_ocorrencia NOT IN ('2025-057682451-001', '2025-008582003-001', '2025-038467267-001', '2025-037825540-001', '2025-039435479-001')
	    AND 	oco.codigo_municipio in (310470,311080,311300,311545,312675,312680,313230,313270,313507,313700,313920,314490,314530,314535,314620,314850,315000,315240,316330,316555,316860)
			-- Filtra apenas ocorrências fechadas
	   -- AND OCO.codigo_municipio IN (123456,456789,987654,......) -- PARA RESGATAR APENAS OS DADOS DOS MUNICÍPIOS SOB SUA RESPONSABILIDADE, REMOVA O COMENTÁRIO E ADICIONE O CÓDIGO DE MUNICIPIO DA SUA RESPONSABILIDADE. NO INÍCIO DO SCRIPT, É POSSÍVEL VERIFICAR ESSES CÓDIGOS, POR RPM E UEOP.
	   -- AND OCO.unidade_area_militar_nome LIKE '%x BPM/x RPM%' -- Filtra pelo nome da unidade área militar
	ORDER BY RPM_2024, UEOP_2024, OCO.data_hora_fato,              -- Ordena por RPM, UEOP, data/hora
	         OCO.numero_ocorrencia, ENV.nome_completo_envolvido,    -- Número da ocorrência, nome do envolvido
	         ENV.nome_mae, ENV.data_nascimento;                     -- Nome da mãe e data de nascimento
    