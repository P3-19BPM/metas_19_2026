SELECT OCO.numero_ocorrencia,   -- Número da ocorrência                                      
OCO.natureza_codigo,                                         -- Código da natureza da ocorrência
OCO.natureza_descricao,                                      -- Descrição da natureza da ocorrência
OCO.local_imediato_codigo,  								 -- Código do local imediato
OCO.local_imediato_descricao,								 -- Descrição do local imediato
OCO.complemento_natureza_codigo,							 -- Código do complemento da natureza da ocorrência
OCO.complemento_natureza_descricao,							 -- Descrição do complemento da natureza da ocorrência
CASE
    WHEN OCO.codigo_municipio IN (310090 , 310100 , 310170 , 310270 , 310340 , 310470 , 310520 , 310660 , 311080 , 311300 , 311370 , 311545 , 311700 , 311950 , 312015 , 312235 , 312245 , 312560 , 312675 , 312680 , 312705 , 313230 , 313270 , 313330 , 313400 , 313470 , 313507 , 313580 , 313600 , 313650 , 313700 , 313890 , 313920 , 314055 , 314140 , 314315 , 314430 , 314490 , 314530 , 314535 , 314620 , 314630 , 314675 , 314850 , 314870 , 315000 , 315217 , 315240 , 315510 , 315660 , 315710 , 315765 , 315810 , 316030 , 316330 , 316555 , 316670 , 316860 , 317030 , 317160) THEN '15 RPM'   
END AS RPM_2025_AREA,
CASE 
    WHEN OCO.codigo_municipio IN (310470,311080,311300,311545,312675,312680,313230,313270,313507,313700,313920,314490,314530,314535,314620,314850,315000,315240,316330,316555,316860) THEN '19 BPM'
    WHEN OCO.codigo_municipio in (310090,310660,311370,312015,312705,313890,314430,315765,316670,317030) THEN '24 CIA PM IND'
    WHEN OCO.codigo_municipio in (310170,310520,312245,312560,313470,313580,313600,313650,314055,314315,314675,315510,315660,315710,315810,316030,310100,310270,312235,314870) THEN '44 BPM'
    WHEN OCO.codigo_municipio in (310340,311950,313400,314630,317160,311700,313330,314140,315217) THEN '70 BPM'
    ELSE 'OUTROS'
END AS UEOP_2025_AREA,
LO.codigo_unidade_area,										-- Código da unidade militar da área
LO.unidade_area_militar_nome,                                -- Nome da unidade militar da área
OCO.unidade_responsavel_registro_codigo,                      -- Código da unidade que registrou a ocorrência
OCO.unidade_responsavel_registro_nome,                        -- Nome da unidade que registrou a ocorrência
SPLIT_PART(OCO.unidade_responsavel_registro_nome,'/',-1) RPM_REGISTRO, 
SPLIT_PART(OCO.unidade_responsavel_registro_nome,'/',-2) UEOP_REGISTRO, 
CASE 																			-- se o território é Urbano ou Rural segundo o IBGE
    	WHEN OCO.pais_codigo <> 1 AND OCO.ocorrencia_uf IS NULL THEN 'Outro_Pais'  	-- trata erro - ocorrencia de fora do Brasil
		WHEN OCO.ocorrencia_uf <> 'MG' THEN 'Outra_UF'								-- trata erro - ocorrencia de fora de MG
    	WHEN OCO.numero_latitude IS NULL THEN 'Invalido'							-- trata erro - ocorrencia sem latitude
        WHEN geo.situacao_codigo = 9 THEN 'Agua'									-- trata erro - ocorrencia dentro de curso d'água
       	WHEN geo.situacao_zona IS NULL THEN 'Erro_Processamento'					-- checa se restou alguma ocorrencia com erro
    	ELSE geo.situacao_zona
END AS situacao_zona,  
CAST(OCO.codigo_municipio AS INTEGER) codigo_municipio,                        -- Converte o código do município para número inteiro
OCO.nome_municipio,                                           -- Nome do município da ocorrência
OCO.tipo_logradouro_descricao,                                -- Tipo do logradouro (Rua, Avenida, etc)
OCO.logradouro_nome,                                          -- Nome do logradouro
OCO.numero_endereco,                                          -- Número do endereço
OCO.nome_bairro,                                              -- Nome do bairro
OCO.ocorrencia_uf,                                            -- Estado da ocorrência
REPLACE(CAST(OCO.numero_latitude AS STRING), ".", ",") AS local_latitude_formatado,
REPLACE(CAST(OCO.numero_longitude AS STRING), ".", ",") AS local_longitude_formatado,
CONCAT(
    SUBSTR(CAST(OCO.data_hora_fato AS STRING), 9, 2), '/',  -- Dia (posições 9-10)
    SUBSTR(CAST(OCO.data_hora_fato AS STRING), 6, 2), '/',  -- Mês (posições 6-7)
    SUBSTR(CAST(OCO.data_hora_fato AS STRING), 1, 4), ' ',  -- Ano (posições 1-4)
    SUBSTR(CAST(OCO.data_hora_fato AS STRING), 12, 8)       -- Hora (posições 12-19)
  ) AS data_hora_fato2,                   -- Converte a data/hora do fato para o padrão brasileiro
YEAR(OCO.data_hora_fato) AS ano,                           -- Ano do fato
MONTH(OCO.data_hora_fato) AS mes, -- Mês do fato
OCO.data_hora_fato,
geo.latitude_sirgas2000  AS numero_latitude,                                          -- Latitude da localização
  geo.longitude_sirgas2000 AS numero_longitude,                                         -- Longitude da localização
  geo.latitude_sirgas2000,				-- reprojeção da latitude de SAD69 para SIRGAS2000
  geo.longitude_sirgas2000,				-- reprojeção da longitude de SAD69 para SIRGAS2000
OCO.nome_tipo_relatorio,                                   -- Tipo do relatório
OCO.digitador_sigla_orgao                                  -- Sigla do órgão que registrou
FROM db_bisp_reds_reporting.tb_ocorrencia OCO
LEFT JOIN db_bisp_reds_master.tb_local_unidade_area_pmmg LO ON OCO.id_local = LO.id_local
LEFT JOIN db_bisp_reds_master.tb_ocorrencia_setores_geodata AS geo ON OCO.numero_ocorrencia = geo.numero_ocorrencia AND OCO.ocorrencia_uf = 'MG'	-- Tabela de apoio que compara as lat/long com os setores IBGE		
WHERE 1 = 1   -- Condição invariavelmente verdadeira que serve como ponto de partida para a cláusula WHERE, facilitando adições ou remoções futuras
AND YEAR(OCO.data_hora_fato) >=2026
--AND OCO.data_hora_fato BETWEEN '2024-01-01 00:00:00.000' AND '2025-02-28 23:59:59.000' -- Delimitação temporal das ocorrências, selecionando fatos ocorridos entre janeiro/2024 e fevereiro/2025
AND OCO.natureza_codigo = 'C01155'                                         -- Filtragem por ocorrência  de natureza C01155 - Furto
AND (
      ((SUBSTRING(OCO.local_imediato_codigo , 1, 2) IN ('07', '10', '14', '15', '03')) OR OCO.local_imediato_codigo = '0512')
		AND OCO.complemento_natureza_codigo IN ('2002', '2004', '2005', '2015')
) -- Filtro por códigos de complemento da natureza 
AND OCO.ocorrencia_uf = 'MG'          -- Filtra apenas ocorrências do estado de Minas Gerais                      
AND OCO.codigo_municipio IN (310340,311950,313400,314630,317160,311700,313330,314140,315217, -- '70 BPM'
							        310470,311080,311300,311545,312675,312680,313230,313270,313507,313700,313920,314490,314530,314535,314620,314850,315000,315240,316330,316555,316860, -- '19 BPM'
							       310090,310660,311370,312015,312705,313890,314430,315765,316670,317030, -- '24 CIA PM IND'
							       310170,310520,312245,312560,313470,313580,313600,313650,314055,314315,314675,315510,315660,315710,315810,316030,310100,310270,312235,314870) -- '44 BPM'   
AND OCO.digitador_sigla_orgao  IN ('PM','PC') -- Filtro por ocorrências, Polícia Militar ou Polícia Civil
AND OCO.ind_estado = 'F'                                -- Filtra apenas ocorrências fechadas
--AND OCO.unidade_area_militar_nome LIKE '%X BPM/X RPM%'   -- FILTRE PELO NOME DA UNIDADE AREA MILITAR
ORDER BY OCO.numero_ocorrencia




