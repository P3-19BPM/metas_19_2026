SELECT 
		OCO.numero_ocorrencia, --IRTD - Indicador de Resposta ao Tráfico de Drogas
	    OCO.natureza_codigo, 
	    CASE 
	        WHEN OCO.codigo_municipio IN (310470,311080,311300,311545,312675,312680,313230,313270,313507,313700,313920,314490,314530,314535,314620,314850,315000,315240,316330,316555,316860) THEN '19 BPM'
	        WHEN OCO.codigo_municipio IN (310090,310660,311370,312015,312705,313890,314430,315765,316670,317030) THEN '24 CIA PM IND'
	        WHEN OCO.codigo_municipio IN (310340,311950,313400,314630,317160,311700,313330,314140,315217) THEN '70 BPM'
	        WHEN OCO.codigo_municipio IN (310170,310520,312245,312560,313470,313580,313600,313650,314055,314315,314675,315510,315660,315710,315810,316030,310100,310270,312235,314870) THEN '44 BPM'
	        ELSE 'OUTROS'
	    END AS UEOP,	
	    CASE-- Criando coluna de CIA PM
		    WHEN OCO.numero_ocorrencia = '2025-004033880-001' THEN '74 CIA TM'
	        WHEN OCO.unidade_responsavel_registro_nome LIKE '%74 CIA TM%' THEN '74 CIA TM' 
	        WHEN OCO.unidade_responsavel_registro_nome LIKE '%19 BPM%' THEN OCO.unidade_responsavel_registro_nome             
	        ELSE 'OUTRA'
	    END AS cia_pm_registro,
	    OCO.data_hora_fato, 
		OCO.nome_municipio,
		OCO.numero_latitude,
	    OCO.numero_longitude,
		COUNT(CASE WHEN ENV.id_tipo_prisao_apreensao IN (1,2,3,4,6,7) THEN 1 END) AS QTD_PRESOS
	FROM db_bisp_reds_reporting.tb_ocorrencia OCO
		left join db_bisp_reds_reporting.tb_envolvido_ocorrencia ENV
		ON ENV.numero_ocorrencia = OCO.numero_ocorrencia 
	WHERE (OCO.natureza_codigo IN ('I04033')
            OR OCO.natureza_secundaria1_codigo IN ('I04033')
            OR OCO.natureza_secundaria2_codigo IN ('I04033')
            OR OCO.natureza_secundaria3_codigo IN ('I04033'))
	  	AND YEAR(OCO.data_hora_fato) >= 2026
		AND OCO.ind_estado = 'F'                                       -- Filtra apenas ocorrências finalizadas
	  	AND (OCO.unidade_responsavel_registro_nome LIKE '%19 BPM%'  OR OCO.numero_ocorrencia IN ('2025-009584040-001'))
		AND OCO.codigo_municipio IN (310470,311080,311300,311545,312675,312680,313230,313270,313507,313700,313920,314490,314530,314535,314620,314850,315000,315240,316330,316555,316860)
	GROUP BY 
	    OCO.numero_ocorrencia, 
	    OCO.natureza_codigo, 
	    OCO.codigo_municipio,
	    cia_pm_registro,
	    OCO.data_hora_fato, 
	    OCO.nome_municipio,
	   	OCO.numero_latitude,
	    OCO.numero_longitude;
