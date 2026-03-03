WITH LETALIDADE AS -- Define uma tabela temporária para filtrar ocorrências com letalidade policial
(
    SELECT -- Inicia a seleção dos campos para a CTE
        ENV.numero_ocorrencia, -- Seleciona o número identificador da ocorrência
        ENV.digitador_id_orgao, -- Seleciona o ID do órgão que digitou a ocorrência
        ENV.natureza_ocorrencia_codigo, -- Seleciona o código da natureza da ocorrência
        ENV.data_hora_fato, -- Seleciona a data e hora do fato
        ENV.ind_militar_policial_servico -- Seleciona o indicador se o militar estava em serviço
    FROM
        db_bisp_reds_reporting.tb_envolvido_ocorrencia ENV -- Define a tabela fonte dos dados de envolvidos
    WHERE
        1 = 1 -- Inicia as condições de filtro
        AND ENV.natureza_ocorrencia_codigo IN ('B01121', 'B01129') -- Filtra por natureza específica de letalidade (Homicídio)
        AND ENV.id_envolvimento IN (35, 36, 44) -- Filtra pelos tipos de envolvimento (autor, co-autor, suspeito)
        AND ENV.ind_militar_policial IS NOT DISTINCT FROM 'M' -- Filtra apenas militares
        AND ENV.ind_militar_policial_servico IS NOT DISTINCT FROM 'S' -- Filtra apenas militares em serviço
        AND YEAR(ENV.data_hora_fato) = 2016 --:ANO -- Filtra pelo ano parametrizado
        AND MONTH(ENV.data_hora_fato) >= 1 --:MESINICIAL -- Filtra a partir do mês inicial parametrizado
        AND MONTH(ENV.data_hora_fato) <= 12 --:MESFINAL -- Filtra até o mês final parametrizado
)
SELECT -- Inicia a seleção principal da query
    OCO.numero_ocorrencia, -- Número identificador da ocorrência
    ENV.envolvimento_codigo, -- Código do tipo de envolvimento na ocorrência
    ENV.envolvimento_descricao, -- Descrição do tipo de envolvimento
    ENV.numero_envolvido, -- Número identificador do envolvido
    ENV.nome_completo_envolvido, -- Nome completo do envolvido
    ENV.nome_mae, -- Nome da mãe do envolvido
    ENV.data_nascimento, -- Data de nascimento do envolvido
    ENV.ind_militar_policial_servico, -- Indicador se militar estava em serviço
    ENV.condicao_fisica_descricao, -- Descrição da condição física do envolvido
    CASE
        WHEN OCO.numero_ocorrencia IN ('2025-047221801-001') THEN 'B01121'
        ELSE ENV.natureza_ocorrencia_codigo
    END natureza_ocorrencia_codigo, -- Código da natureza da ocorrência
    ENV.natureza_ocorrencia_descricao, -- Descrição da natureza da ocorrência
    ENV.ind_consumado, -- Indicador se o crime foi consumado ou tentado
    CASE
        WHEN OCO.codigo_municipio IN (310090, 310100, 310170, 310270, 310340, 310470, 310520, 310660, 311080, 311300, 311370, 311545, 311700, 311950, 312015, 312235, 312245, 312560, 312675, 312680, 312705, 313230, 313270, 313330, 313400, 313470, 313507, 313580, 313600, 313650, 313700, 313890, 313920, 314055, 314140, 314315, 314430, 314490, 314530, 314535, 314620, 314630, 314675, 314850, 314870, 315000, 315217, 315240, 315510, 315660, 315710, 315765, 315810, 316030, 316330, 316555, 316670, 316860, 317030, 317160) THEN '15 RPM'
        ELSE 'OUTROS'
    END AS RPM_2024,
    CASE
        WHEN OCO.codigo_municipio IN (310470, 311080, 311300, 311545, 312675, 312680, 313230, 313270, 313507, 313700, 313920, 314490, 314530, 314535, 314620, 314850, 315000, 315240, 316330, 316555, 316860) THEN '19 BPM'
        ELSE 'OUTROS'
    END AS UEOP_2024,
    OCO.unidade_area_militar_codigo, -- Código da unidade militar da área
    OCO.unidade_area_militar_nome, -- Nome da unidade militar da área
    OCO.unidade_responsavel_registro_codigo, -- Código da unidade que registrou a ocorrência
    OCO.unidade_responsavel_registro_nome, -- Nome da unidade que registrou a ocorrência
    CAST(OCO.codigo_municipio AS INTEGER), -- Converte o código do município para número inteiro
    OCO.nome_municipio, -- Nome do município da ocorrência
    OCO.tipo_logradouro_descricao, -- Tipo do logradouro (Rua, Avenida, etc)
    OCO.logradouro_nome, -- Nome do logradouro
    OCO.numero_endereco, -- Número do endereço
    OCO.nome_bairro, -- Nome do bairro
    OCO.ocorrencia_uf, -- Estado da ocorrência
    -- TRATATIVA PARA FORÇAR COORDENADAS DA OCORRÊNCIA ESPECÍFICA
    CASE 
        WHEN OCO.numero_ocorrencia = '2026-006053949-001' THEN -17.915675046083376
        ELSE OCO.numero_latitude 
    END AS numero_latitude, -- Latitude da localização
    
    CASE 
        WHEN OCO.numero_ocorrencia = '2026-006053949-001' THEN -41.49574549934725
        ELSE OCO.numero_longitude 
    END AS numero_longitude, -- Longitude da localização
    OCO.data_hora_fato, -- Data e hora do fato
    YEAR(OCO.data_hora_fato) AS ano, -- Ano do fato
    MONTH(OCO.data_hora_fato) AS mes, -- Mês do fato
    OCO.nome_tipo_relatorio, -- Tipo do relatório
    OCO.digitador_sigla_orgao -- Sigla do órgão que registrou
FROM
    db_bisp_reds_reporting.tb_ocorrencia AS OCO -- Tabela principal de ocorrências
    INNER JOIN db_bisp_reds_reporting.tb_envolvido_ocorrencia AS ENV -- Join com tabela de envolvidos
    ON OCO.numero_ocorrencia = ENV.numero_ocorrencia
    LEFT JOIN LETALIDADE LET -- Join com a CTE de letalidade
    ON OCO.numero_ocorrencia = LET.numero_ocorrencia
WHERE
    -- Bloco de condição para a ocorrência específica, que ignora todas as outras regras
    ( OCO.numero_ocorrencia IN ('2025-047221801-001') AND ENV.id_envolvimento IN (25, 32, 1097, 26, 27, 28, 872))
    OR -- OU a ocorrência atende a todas as regras gerais abaixo
    (
        LET.numero_ocorrencia IS NULL -- Exclui ocorrências que estão na CTE de letalidade
        AND ENV.id_envolvimento IN (25, 32, 1097, 26, 27, 28, 872) -- Filtra tipos específicos de envolvimento(Todos vitima)
        AND ENV.natureza_ocorrencia_codigo IN ('B01121', 'B01148', 'B02001', 'B01504') -- Filtra naturezas específicas(Homicídio,Sequestro e Cárcere Privado,Tortura, Feminicídio*)
        AND ENV.ind_consumado IN ('S', 'N') -- Filtra ocorrências consumadas e tentadas
        AND oco.numero_ocorrencia NOT IN ('2025-025232870-001', '2025-025472623-001', '2025-059321475-002')
        AND ENV.condicao_fisica_codigo IS DISTINCT FROM '0100' -- Exclui condição física específica(Fatal)
        AND OCO.ocorrencia_uf = 'MG' -- Filtra apenas ocorrências de Minas Gerais
        AND OCO.digitador_sigla_orgao IN ('PM', 'PC') -- Filtra registros da PM ou PC
        AND OCO.nome_tipo_relatorio IN ('POLICIAL', 'REFAP') -- Filtra tipos específicos de relatório
        AND YEAR(OCO.data_hora_fato) >= 2026 --:ANO -- Filtra pelo ano parametrizado
        AND MONTH(OCO.data_hora_fato) >= 1 --:MESINICIAL -- Filtra a partir do mês inicial
        AND MONTH(OCO.data_hora_fato) <= 12 --:MESFINAL -- Filtra até o mês final
        AND OCO.ind_estado = 'F' -- Filtra apenas ocorrências finalizadas
        AND OCO.codigo_municipio IN (310470, 311080, 311300, 311545, 312675, 312680, 313230, 313270, 313507, 313700, 313920, 314490, 314530, 314535, 314620, 314850, 315000, 315240, 316330, 316555, 316860)
        -- PARA RESGATAR APENAS OS DADOS DOS MUNICÍPIOS SOB SUA RESPONSABILIDADE, REMOVA O COMENTÁRIO E ADICIONE O CÓDIGO DE MUNICIPIO DA SUA RESPONSABILIDADE. NO INÍCIO DO SCRIPT, É POSSÍVEL VERIFICAR ESSES CÓDIGOS, POR RPM E UEOP.
        -- AND OCO.unidade_area_militar_nome LIKE '%x BPM/x RPM%' -- Filtra pelo nome da unidade área militar
    )
ORDER BY -- Define a ordem de apresentação dos resultados
    RPM_2024, -- Primeiro por RPM
    UEOP_2024, -- Depois por UEOP
    OCO.data_hora_fato, -- Depois por data/hora
    OCO.numero_ocorrencia, -- Depois por número da ocorrência
    ENV.nome_completo_envolvido, -- Depois por nome do envolvido
    ENV.nome_mae, -- Depois por nome da mãe
    ENV.data_nascimento; -- Por fim, por data de nascimento