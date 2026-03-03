# E:\GitHub\relatorios_GDO_GDI\process_impala_data.py
import os
import sys  # Adicione no topo do arquivo
import json  # Adicione no topo do arquivo
import pyodbc
import pandas as pd
import geopandas as gpd
import openpyxl
from dotenv import load_dotenv
from shapely.geometry import Point
from openpyxl import load_workbook
from datetime import datetime  # <--- ADICIONADO
import locale  # <--- ADICIONADO
import re

# --- ROTAS (BISP) ---
from mmpg_netroute import ensure_host_route  # importa seu helper
BISP_HOSTNAME = "dlmg.prodemge.gov.br"
# pode definir no .env se quiser
BISP_NEXT_HOP = os.getenv("BISP_NEXT_HOP", "10.14.56.1")

# --- FUNÇÃO AUXILIAR PARA PORTABILIDADE (PyInstaller) ---


def resource_path(relative_path):
    """ Obtém o caminho absoluto para um recurso, funciona para dev e para o PyInstaller """
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


# --- 1. CONFIGURAÇÕES E CAMINHOS ---
# Define o local para português do Brasil para formatar o nome do mês corretamente
try:
    locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')
except locale.Error:
    locale.setlocale(locale.LC_TIME, 'Portuguese_Brazil')

load_dotenv()
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')

sql_scripts_folder = resource_path("sql_scripts")
geojson_path = resource_path(os.path.join(
    "mapas", "SubSetores_19BPM_GeoJSON.json"))
cert_path = resource_path(os.path.join("Certificados", "cacerts.pem"))
xlsx_dir = resource_path("xlsx")

# Caminho para o arquivo de mensagem
mensagem_md_path = resource_path("Mensagem_PA.md")  # <--- ADICIONADO

output_dir = "output"
csv_dir = "csv"
os.makedirs(output_dir, exist_ok=True)
os.makedirs(csv_dir, exist_ok=True)
print(f"Arquivos de saida serao salvos em: {os.path.abspath(output_dir)}")


# --- 2. DEFINIÇÃO DAS TAREFAS ---
gdo_sql_files_mapping = {
    "BD_IMV2025": "BD_IMV2025.sql",
    "BD_ICVPe": "BD_ICVPe.sql",
    "BD_ICVPa": "BD_ICVPa.sql",
    "BD_POG": "BD_POG.sql",
    "BD_PL": "BD_PL.sql",
    "BD_IRTD": "BD_IRTD.sql"
}

int_cum_sql_files_mapping = {
    "BD_MRPP_INT_CUM": "BD_MRPP_INT_CUM.sql",
    "BD_RC_INT_CUM": "BD_RC_INT_CUM.sql",
    "BD_VCP_INT_CUM": "BD_VCP_INT_CUM.sql",
    "BD_VTC_DENOMINADOR_INT_CUM": "BD_VTC_DENOMINADOR_INT_CUM.sql",
    "BD_VTC_NUMERADOR_INT_CUM": "BD_VTC_NUMERADOR_INT_CUM.sql",
    "BD_VT_DENOMINADOR_INT_CUM": "BD_VT_DENOMINADOR_INT_CUM.sql",
    "BD_VT_NUMERADOR_INT_CUM": "BD_VT_NUMERADOR_INT_CUM.sql"
}

# --- 3. FUNÇÕES DE PROCESSAMENTO ---


def fetch_data_from_impala(sql_query, user, pwd):
    """ Conecta ao Impala usando a string DSN-less e executa uma consulta. """

    # ---> GARANTE A ROTA PARA O HOST DA BISP <---
    try:
        print(
            f"   - Garantindo rota para {BISP_HOSTNAME} via {BISP_NEXT_HOP}...")
        ip_resolvido = ensure_host_route(BISP_HOSTNAME, BISP_NEXT_HOP)
        if not ip_resolvido:
            print(
                "   - [AVISO] Nao foi possivel garantir a rota; tentando conectar assim mesmo.")
        else:
            print(f"   - Rota ok. Destino atual: {ip_resolvido}")
    except Exception as e:
        print(
            f"   - [AVISO] Falha ao garantir rota ({e}). Tentando conectar assim mesmo.")

    conn = None
    connection_string = (
        f"Driver={{Cloudera ODBC Driver for Impala}};"
        f"Host={BISP_HOSTNAME};"
        f"Port=21051;"
        f"AuthMech=3;"
        f"UID={user};"
        f"PWD={pwd};"
        f"TransportMode=sasl;"
        f"KrbServiceName=impala;"
        f"SSL=1;"
        f"AllowSelfSignedServerCert=1;"
        f"TrustedCerts={cert_path};"
        f"AutoReconnect=1;"
        f"UseSQLUnicode=1;"
    )
    try:
        print("   - Conectando ao Impala...")
        conn = pyodbc.connect(connection_string, autocommit=True)
        print("   - Executando consulta SQL...")
        df = pd.read_sql(sql_query, conn)
        print(f"   - Consulta finalizada: {df.shape[0]} registros obtidos.")
        return df
    finally:
        if conn:
            conn.close()


def process_dataframe_for_spatial_join(df, geojson_path_local):
    """ Realiza a junção geoespacial, garantindo que NENHUMA linha seja perdida. """
    print("   - Iniciando processamento espacial...")
    original_df = df.copy()
    spatial_columns_to_add = ['geometry', 'SubSetor', 'Pelotao', 'CIA_PM']

    try:
        required_coords = ['numero_latitude', 'numero_longitude']
        if not all(col in df.columns for col in required_coords):
            print(
                "   - [AVISO] Colunas de coordenadas nao encontradas. Adicionando colunas espaciais em branco.")
            for col in spatial_columns_to_add:
                original_df[col] = None
            return original_df

        df_coords = original_df.copy()
        df_coords['numero_latitude'] = pd.to_numeric(
            df_coords['numero_latitude'], errors='coerce')
        df_coords['numero_longitude'] = pd.to_numeric(
            df_coords['numero_longitude'], errors='coerce')
        df_coords.dropna(subset=required_coords, inplace=True)

        if df_coords.empty:
            print(
                "   - [AVISO] Nenhum registro com coordenadas validas. Adicionando colunas espaciais em branco.")
            for col in spatial_columns_to_add:
                original_df[col] = None
            return original_df

        print(
            f"   - Processando {len(df_coords)} de {len(original_df)} registros que possuem coordenadas validas...")
        geometry = [Point(xy) for xy in zip(
            df_coords['numero_longitude'], df_coords['numero_latitude'])]
        points_gdf = gpd.GeoDataFrame(
            df_coords, geometry=geometry, crs='EPSG:4326')

        polygons_gdf = gpd.read_file(geojson_path_local)
        if polygons_gdf.crs != points_gdf.crs:
            polygons_gdf = polygons_gdf.to_crs(points_gdf.crs)

        spatial_join_result = gpd.sjoin(
            points_gdf, polygons_gdf, how='left', predicate='within')
        spatial_join_result.rename(
            columns={'name': 'SubSetor', 'PELOTAO': 'Pelotao', 'CIA_PM': 'CIA_PM'}, inplace=True)

        columns_to_merge = [
            col for col in spatial_columns_to_add if col in spatial_join_result.columns]

        if 'geometry' in spatial_join_result.columns:
            spatial_join_result['geometry'] = spatial_join_result['geometry'].astype(
                str)

        final_df = original_df.merge(
            spatial_join_result[columns_to_merge], left_index=True, right_index=True, how='left')

        print("   - Juncao espacial concluida. Todas as linhas originais foram mantidas.")

        if 'data_hora_fato' in final_df.columns:
            final_df['data_fato'] = pd.to_datetime(
                final_df['data_hora_fato'], errors='coerce').dt.date

        return final_df
    except Exception as e:
        print(
            f"- [ERRO INESPERADO] Falha na juncao: {e}. Retornando dados originais com colunas em branco.")
        for col in spatial_columns_to_add:
            original_df[col] = None
        return original_df


# Confirme que esta função está assim no seu arquivo
def process_data_set(sql_mapping, geojson_path_local, user, pwd, dataset_name=""):
    """
    Orquestra o processo para um conjunto de dados, incluindo a criação de um arquivo de trava.
    """
    print(f"\n{'='*50}\nIniciando processamento para: {dataset_name}\n{'='*50}")

    excel_filename = f"Monitoramento_{dataset_name}.xlsx"
    excel_output_path = os.path.join(xlsx_dir, excel_filename)
    lock_file_path = excel_output_path + ".lock"

    if os.path.exists(lock_file_path):
        os.remove(lock_file_path)
        print(f"Arquivo de trava antigo removido: {lock_file_path}")

    try:
        # >>>>> ESTA É A CONFIGURAÇÃO SEGURA E CORRETA <<<<<
        with pd.ExcelWriter(
            excel_output_path,
            engine='openpyxl',
            mode='a',  # 'a' de "append" (acrescentar), não apaga o arquivo.
            if_sheet_exists='replace' # Substitui apenas as abas com o mesmo nome.
        ) as writer:
            for sheet_name, sql_filename in sql_mapping.items():
                print(f"\n-> Processando aba: {sheet_name}")
                sql_filepath = os.path.join(sql_scripts_folder, sql_filename)

                try:
                    # ... (o resto do código da função permanece o mesmo)
                    with open(sql_filepath, 'r', encoding='utf-8') as f:
                        sql_query = f.read()
                    raw_data_df = fetch_data_from_impala(sql_query, user, pwd)
                    csv_output_path = os.path.join(csv_dir, f"{sheet_name}.csv")
                    raw_data_df.to_csv(csv_output_path, index=False, encoding='utf-8-sig')
                    print(f"   - Dados brutos salvos em: {csv_output_path}")
                    processed_data = process_dataframe_for_spatial_join(raw_data_df, geojson_path_local)
                    if 'geometry' in processed_data.columns:
                        processed_data['geometry'] = processed_data['geometry'].astype(str)
                    print(f"   - Salvando dados na planilha '{sheet_name}'...")
                    processed_data.to_excel(writer, sheet_name=sheet_name, index=False)
                except Exception as e:
                    print(f"   - [ERRO] Falha ao processar a aba '{sheet_name}': {e}")
                    continue

        print(f"\nArquivo '{os.path.basename(excel_output_path)}' atualizado com sucesso!")

        with open(lock_file_path, 'w') as f:
            pass
        print(f"Arquivo de trava criado: {lock_file_path}")

    except FileNotFoundError:
        print(f"\n[ERRO FATAL] O arquivo de template Excel '{excel_output_path}' nao foi encontrado.")
    except Exception as e:
        print(f"\n[ERRO FATAL] Falha ao escrever no arquivo Excel '{excel_output_path}': {e}")

# --- 4. FUNÇÃO DE ATUALIZAÇÃO DA MENSAGEM (NOVA SEÇÃO) ---


def atualizar_mensagem_md(user, pwd):
    """
    Busca a data de atualização do BISP, gera a saudação e atualiza o arquivo Mensagem_PA.md
    identificando e substituindo as linhas corretas, independentemente do conteúdo anterior.
    """
    print(f"\n{'='*50}\nIniciando atualizacao do arquivo de mensagem\n{'='*50}")

    hora_atual = datetime.now().hour
    if 0 <= hora_atual < 12: saudacao = "Bom dia!"
    elif 12 <= hora_atual < 18: saudacao = "Boa tarde!"
    else: saudacao = "Boa noite!"
    print(f"   - Saudacao definida como: {saudacao}")

    try:
        sql_bisp_path = os.path.join(sql_scripts_folder, "Atualizacao_BISP.sql")
        with open(sql_bisp_path, 'r', encoding='utf-8') as f: sql_query = f.read()
        df_data = fetch_data_from_impala(sql_query, user, pwd)
        if df_data.empty or pd.isna(df_data.iloc[0, 0]):
            print("   - [ERRO] A consulta de data de atualizacao nao retornou valor.")
            return
        timestamp_atualizacao = pd.to_datetime(df_data.iloc[0, 0])
        data_formatada = timestamp_atualizacao.strftime("%d de %B de %Y").lower()
        timestamp_original = timestamp_atualizacao.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        texto_data = f"{data_formatada} - *({timestamp_original})*"
        print(f"   - Texto de data gerado: {texto_data}")
    except Exception as e:
        print(f"   - [ERRO FATAL] Nao foi possivel obter a data de atualizacao do BISP: {e}")
        return

    try:
        with open(mensagem_md_path, 'r', encoding='utf-8') as f:
            linhas = f.readlines()

        # Abordagem mais segura: iterar pelas linhas
        novas_linhas = []
        for linha in linhas:
            if linha.strip().startswith("#### Senhores,"):
                novas_linhas.append(f"#### Senhores, {saudacao}\n")
            elif linha.strip().startswith("Encaminho o monitoramento"):
                novas_linhas.append(f"Encaminho o monitoramento da **GDO**, do **Plano de Ação, PPAG e GDI,** com os dados até {texto_data} .\n")
            else:
                novas_linhas.append(linha)

        with open(mensagem_md_path, 'w', encoding='utf-8') as f:
            f.writelines(novas_linhas)

        print(f"   - Arquivo '{os.path.basename(mensagem_md_path)}' atualizado com sucesso!")
    except FileNotFoundError:
        print(f"   - [ERRO FATAL] Arquivo de template de mensagem nao encontrado em: {mensagem_md_path}")
    except Exception as e:
        print(f"   - [ERRO FATAL] Falha ao escrever no arquivo de mensagem: {e}")


# --- 5. EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    if not username or not password:
        print("ERRO CRITICO: As variaveis DB_USERNAME e DB_PASSWORD nao foram encontradas.")
        print("   Verifique se o arquivo '.env' existe e esta preenchido.")
        sys.exit(1)  # Use sys.exit para parar a execução

    # --- LÓGICA DE SELEÇÃO DE CONSULTAS ---
    gdo_to_run = gdo_sql_files_mapping.copy()
    int_to_run = int_cum_sql_files_mapping.copy()

    # Verifica se o script foi chamado com argumentos da GUI
    if len(sys.argv) > 1:
        try:
            selections = json.loads(sys.argv[1])
            selected_gdo = selections.get("gdo", [])
            selected_int = selections.get("int", [])

            print("Execução via GUI detectada. Filtrando consultas...")

            if selected_gdo:
                gdo_to_run = {
                    k: v for k, v in gdo_sql_files_mapping.items() if k in selected_gdo}
            else:
                gdo_to_run = {}  # Não executa nada de GDO se a lista estiver vazia

            if selected_int:
                int_to_run = {
                    k: v for k, v in int_cum_sql_files_mapping.items() if k in selected_int}
            else:
                int_to_run = {}  # Não executa nada de INT se a lista estiver vazia

        except (json.JSONDecodeError, IndexError) as e:
            print(
                f"[AVISO] Argumento de seleção inválido ({e}). Executando todas as consultas como padrão.")
            # Se o argumento for inválido, ele executa tudo como antes.
    else:
        print("Nenhuma seleção da GUI recebida. Executando todas as consultas como padrão.")

    # --- PROCESSAMENTO DOS DADOS ---

    # Processar dados GDO se houver algum selecionado
    if gdo_to_run:
        process_data_set(
            sql_mapping=gdo_to_run,
            geojson_path_local=geojson_path,
            user=username,
            pwd=password,
            dataset_name="GDO_2026"
        )
    else:
        print("\nNenhuma consulta GDO selecionada para execução.")

    # Processar dados de Interação Comunitária se houver algum selecionado
    if int_to_run:
        process_data_set(
            sql_mapping=int_to_run,
            geojson_path_local=geojson_path,
            user=username,
            pwd=password,
            dataset_name="INT_Comunitaria_2026"
        )
    else:
        print("\nNenhuma consulta de Interação Comunitária selecionada para execução.")

    # Atualizar o arquivo de mensagem .md (pode ser executado sempre ou condicionalmente)
    # Se houver qualquer consulta executada, atualiza a mensagem.
    if gdo_to_run or int_to_run:
        atualizar_mensagem_md(user=username, pwd=password)
        print("\nProcessamento de extracao de dados e mensagem concluido.")
    else:
        print("\nNenhuma tarefa executada. Processo finalizado.")