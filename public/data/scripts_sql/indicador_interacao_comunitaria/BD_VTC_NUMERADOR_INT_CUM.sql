WITH 
CRIME_VIOLENTO AS (
  SELECT 
    oco.numero_ocorrencia,                             -- Número da ocorrência
    oco.data_hora_fato,                                -- Data/hora do fato
    oco.natureza_codigo                               -- Código da natureza da ocorrência
  FROM db_bisp_reds_reporting.tb_ocorrencia oco 
  WHERE 1=1
  AND YEAR(oco.data_hora_fato) >=2026
-- and oco.data_hora_fato BETWEEN '2024-01-01 00:00:00.000' AND '2025-04-30 23:59:59.000'-- Filtra ocorrências por período específico (todo o ano de 2024 até fevereiro/2025)
    AND oco.natureza_codigo IN('B01121','B01148','B02001','C01157','C01158','D01217','B01504') -- Seleção de naturezas especifícas do CV
    AND oco.ocorrencia_uf = 'MG'         -- Filtra apenas ocorrências do estado de Minas Gerais  
    AND oco.codigo_municipio IN (310340,311950,313400,314630,317160,311700,313330,314140,315217, -- '70 BPM'
							        310470,311080,311300,311545,312675,312680,313230,313270,313507,313700,313920,314490,314530,314535,314620,314850,315000,315240,316330,316555,316860, -- '19 BPM'
							       310090,310660,311370,312015,312705,313890,314430,315765,316670,317030, -- '24 CIA PM IND'
							       310170,310520,312245,312560,313470,313580,313600,313650,314055,314315,314675,315510,315660,315710,315810,316030,310100,310270,312235,314870) -- '44 BPM'                      
    AND oco.digitador_sigla_orgao IN ('PM', 'PC')  -- Filtro por ocorrências, Polícia Militar ou Polícia Civil
    AND oco.ind_estado = 'F'                                                         -- Filtra apenas ocorrências fechadas
),
VISITAS_TRANQUILIZADORAS AS (
SELECT 
	OCO.numero_ocorrencia,   -- Número da ocorrência                                      
	OCO.natureza_codigo,                                         -- Código da natureza da ocorrência
	OCO.natureza_descricao,                                      -- Descrição da natureza da ocorrência
	LO.codigo_unidade_area,										-- Código da unidade militar da área
	LO.unidade_area_militar_nome,                                -- Nome da unidade militar da área
	OCO.unidade_responsavel_registro_codigo,                      -- Código da unidade que registrou a ocorrência
	OCO.unidade_responsavel_registro_nome,                        -- Nome da unidade que registrou a ocorrência
	CAST(OCO.codigo_municipio AS INTEGER) codigo_municipio,                        -- Converte o código do município para número inteiro
	OCO.nome_municipio,                                           -- Nome do município da ocorrência
	OCO.tipo_logradouro_descricao,                                -- Tipo do logradouro (Rua, Avenida, etc)
	OCO.logradouro_nome,                                          -- Nome do logradouro
	OCO.numero_endereco,                                          -- Número do endereço
	OCO.nome_bairro,                                              -- Nome do bairro
	OCO.ocorrencia_uf,                                            -- Estado da ocorrência
	OCO.numero_latitude ,										-- Numero latitude
	OCO.numero_longitude,										-- Numero longitude
	OCO.data_hora_fato,                                        -- Data e hora do fato
	YEAR(OCO.data_hora_fato) AS ano,                           -- Ano do fato
	MONTH(OCO.data_hora_fato) AS mes,                          -- Mês do fato
	OCO.nome_tipo_relatorio,                                   -- Tipo do relatório
	OCO.digitador_sigla_orgao,                                  -- Sigla do órgão que registrou
	OCO.ind_estado,
    REGEXP_EXTRACT(OCO.historico_ocorrencia, '([0-9]{4}-[0-9]{9}-[0-9]{3})', 0) AS numero_reds_furto, 
    OCO.pais_codigo
  FROM db_bisp_reds_reporting.tb_ocorrencia OCO
  LEFT JOIN db_bisp_reds_master.tb_local_unidade_area_pmmg LO ON OCO.id_local = LO.id_local
  WHERE 1=1
  AND YEAR(OCO.data_hora_fato) >=2026
-- AND OCO.data_hora_fato BETWEEN '2024-01-01 00:00:00.000' AND '2025-04-30 23:59:59.000'
    AND OCO.natureza_codigo = 'A20001'
    AND OCO.ocorrencia_uf = 'MG'                                
    AND OCO.digitador_sigla_orgao = 'PM'
    AND OCO.ind_estado IN ('F','R')
    AND OCO.nome_tipo_relatorio IN ('BOS', 'BOS AMPLO')
    AND OCO.historico_ocorrencia LIKE '%20__-%-00%'
    AND OCO.unidade_responsavel_registro_nome NOT LIKE '%IND PE%'
     AND OCO.codigo_municipio IN (310340,311950,313400,314630,317160,311700,313330,314140,315217, -- '70 BPM'
							        310470,311080,311300,311545,312675,312680,313230,313270,313507,313700,313920,314490,314530,314535,314620,314850,315000,315240,316330,316555,316860, -- '19 BPM'
							       310090,310660,311370,312015,312705,313890,314430,315765,316670,317030, -- '24 CIA PM IND'
							       310170,310520,312245,312560,313470,313580,313600,313650,314055,314315,314675,315510,315660,315710,315810,316030,310100,310270,312235,314870) -- '44 BPM'
	AND OCO.unidade_responsavel_registro_nome NOT LIKE '%PVD%'
	AND (
	    OCO.unidade_responsavel_registro_nome NOT REGEXP '/[A-Za-z]'
	    OR OCO.unidade_responsavel_registro_nome LIKE '%/PEL TM%'
	)
	AND (
	    OCO.unidade_responsavel_registro_nome REGEXP '^(SG|PEL|GP)'
	    OR OCO.unidade_responsavel_registro_nome REGEXP '^[^A-Za-z]'
	) -- Filtra apenas unidades com responsabilidade territorial.
)
SELECT 
  VT.numero_ocorrencia,                                 -- Número da ocorrência da visita
  VT.numero_reds_furto,                                 -- Número da ocorrência de furto relacionada                               
  VT.natureza_codigo,                                   -- Código da natureza da visita
  VT.natureza_descricao,                                -- Descrição da natureza da visita
CASE
    WHEN VT.codigo_municipio IN (310090 , 310100 , 310170 , 310270 , 310340 , 310470 , 310520 , 310660 , 311080 , 311300 , 311370 , 311545 , 311700 , 311950 , 312015 , 312235 , 312245 , 312560 , 312675 , 312680 , 312705 , 313230 , 313270 , 313330 , 313400 , 313470 , 313507 , 313580 , 313600 , 313650 , 313700 , 313890 , 313920 , 314055 , 314140 , 314315 , 314430 , 314490 , 314530 , 314535 , 314620 , 314630 , 314675 , 314850 , 314870 , 315000 , 315217 , 315240 , 315510 , 315660 , 315710 , 315765 , 315810 , 316030 , 316330 , 316555 , 316670 , 316860 , 317030 , 317160) THEN '15 RPM'	
END AS RPM_2025_AREA,
CASE 
    WHEN VT.codigo_municipio IN (310470,311080,311300,311545,312675,312680,313230,313270,313507,313700,313920,314490,314530,314535,314620,314850,315000,315240,316330,316555,316860) THEN '19 BPM'
    WHEN VT.codigo_municipio in (310090,310660,311370,312015,312705,313890,314430,315765,316670,317030) THEN '24 CIA PM IND'
    WHEN VT.codigo_municipio in (310170,310520,312245,312560,313470,313580,313600,313650,314055,314315,314675,315510,315660,315710,315810,316030,310100,310270,312235,314870) THEN '44 BPM'
    WHEN VT.codigo_municipio in (310340,311950,313400,314630,317160,311700,313330,314140,315217) THEN '70 BPM'
    ELSE 'OUTROS'
END AS UEOP_2025_AREA,
  VT.codigo_unidade_area,                       -- Código da unidade militar da área
  VT.unidade_area_militar_nome,                         -- Nome da unidade militar da área
  VT.unidade_responsavel_registro_codigo,               -- Código da unidade responsável pelo registro
  VT.unidade_responsavel_registro_nome,                 -- Nome da unidade responsável pelo registro
  SPLIT_PART(VT.unidade_responsavel_registro_nome,'/',-1) AS RPM_REGISTRO,  -- Extrai a RPM do nome da unidade
  SPLIT_PART(VT.unidade_responsavel_registro_nome,'/',-2) AS UEOP_REGISTRO,  -- Extrai a UEOP do nome da unidade
    CASE 																			-- se o território é Urbano ou Rural segundo o IBGE
    	WHEN VT.pais_codigo <> 1 AND VT.ocorrencia_uf IS NULL THEN 'Outro_Pais'  	-- trata erro - ocorrencia de fora do Brasil
		WHEN VT.ocorrencia_uf <> 'MG' THEN 'Outra_UF'								-- trata erro - ocorrencia de fora de MG
    	WHEN VT.numero_latitude IS NULL THEN 'Invalido'							-- trata erro - ocorrencia sem latitude
        WHEN geo.situacao_codigo = 9 THEN 'Agua'									-- trata erro - ocorrencia dentro de curso d'água
       	WHEN geo.situacao_zona IS NULL THEN 'Erro_Processamento'					-- checa se restou alguma ocorrencia com erro
    	ELSE geo.situacao_zona
END AS situacao_zona,  
  VT.codigo_municipio,   								-- Código do município
  VT.nome_municipio,                                    -- Nome do município
  VT.tipo_logradouro_descricao,                         -- Descrição do tipo de logradouro
  VT.logradouro_nome,                                   -- Nome do logradouro
  VT.numero_endereco,                                   -- Número do endereço
  VT.nome_bairro,                                       -- Nome do bairro
  VT.ocorrencia_uf,                                     -- UF da ocorrência
  REPLACE(CAST(VT.numero_latitude AS STRING), '.', ',') AS local_latitude_formatado,  -- Formata latitude com vírgula
  REPLACE(CAST(VT.numero_longitude AS STRING), '.', ',') AS local_longitude_formatado,  -- Formata longitude com vírgula
   CONCAT(
    SUBSTR(CAST(VT.data_hora_fato AS STRING), 9, 2), '/',  -- Dia (posições 9-10)
    SUBSTR(CAST(VT.data_hora_fato AS STRING), 6, 2), '/',  -- Mês (posições 6-7)
    SUBSTR(CAST(VT.data_hora_fato AS STRING), 1, 4), ' ',  -- Ano (posições 1-4)
    SUBSTR(CAST(VT.data_hora_fato AS STRING), 12, 8)       -- Hora (posições 12-19)
  ) AS data_hora_fato2,                   -- Converte a data/hora do fato para o padrão brasileiro
YEAR(VT.data_hora_fato) AS ano,                           -- Ano do fato
MONTH(VT.data_hora_fato) AS mes, -- Mês do fato
VT.data_hora_fato,
  VT.nome_tipo_relatorio,                               -- Nome do tipo de relatório
  VT.digitador_sigla_orgao,                              -- Sigla do órgão que registrou
  geo.latitude_sirgas2000  AS numero_latitude,                                          -- Latitude da localização
  geo.longitude_sirgas2000 AS numero_longitude,                                         -- Longitude da localização
  geo.latitude_sirgas2000,				-- reprojeção da latitude de SAD69 para SIRGAS2000
  geo.longitude_sirgas2000				-- reprojeção da longitude de SAD69 para SIRGAS2000
FROM VISITAS_TRANQUILIZADORAS VT                        -- Tabela base da consulta (visitas)
INNER JOIN CRIME_VIOLENTO CV ON CV.numero_ocorrencia = VT.numero_reds_furto AND CV.data_hora_fato < VT.data_hora_fato  -- Junta com furtos e garante que a visita ocorreu após o CV
LEFT JOIN db_bisp_reds_master.tb_ocorrencia_setores_geodata AS geo ON VT.numero_ocorrencia = geo.numero_ocorrencia AND VT.ocorrencia_uf = 'MG'	-- Tabela de apoio que compara as lat/long com os setores IBGE		
WHERE 1 =1 
AND EXISTS (                            
    SELECT 1                                                                           -- Seleciona apenas um valor constante (otimização de performance)
    FROM db_bisp_reds_reporting.tb_envolvido_ocorrencia envolvido                      -- Tabela que contém informações dos envolvidos nas ocorrências
    WHERE envolvido.numero_ocorrencia = VT.numero_ocorrencia                          -- Correlaciona os envolvidos com a ocorrência principal
    AND (
        envolvido.numero_cpf_cnpj IS NOT NULL                                          -- Filtra envolvidos que possuem CPF/CNPJ preenchido
        OR (
            envolvido.tipo_documento_codigo IN ('0801','0802', '0803', '0809')                                   -- OU filtra por envolvidos com tipos de documento: RG, Carteira de Trabalho, CNH, Carteira de Registro Profissional 
            AND envolvido.numero_documento_id IS NOT NULL                              -- E que tenham um número de documento de identificação preenchido
        )
    )
) -- Verifica a existência de pelo menos um registro na subconsulta, garantindo que há ao menos envolvido cadastrado, com preenchimento do campo CPF ou RG
AND VT.ind_estado IN ('F','R')  -- Filtra ocorrências com indicador de estado 'F' (Fechado) e R(Pendente de Recibo)
AND VT.unidade_responsavel_registro_nome LIKE '%15 RPM%'   -- FILTRE PELO NOME DA UNIDADE RESPONSÁVEL PELO REGISTRO 
