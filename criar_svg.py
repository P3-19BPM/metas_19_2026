import json
import os

# 1. Configurações e Mapeamento de Links
LINKS_MUNICIPIOS = {
    "ATALEIA": "111", "CAMPANARIO": "112", "CARAI": "113", "CATUJI": "114",
    "FRANCISCOPOLIS": "115", "FREI GASPAR": "116", "ITAIPE": "117",
    "ITAMBACURI": "108", "JAMPRUCA": "118", "LADAINHA": "119",
    "MALACACHETA": "109", "NOVA MODICA": "120", "NOVO ORIENTE DE MINAS": "121",
    "NOVO CRUZEIRO": "110", "OURO VERDE DE MINAS": "122", "PESCADOR": "123",
    "PAVAO": "124", "POTE": "125", "SAO JOSE DO DIVINO": "126",
    "SETUBINHA": "127", "TEOFILO OTONI": "107"
}

# Caminhos atualizados para o seu ambiente local
FILE_INPUT = r'E:\GitHub\metas_19_2026\public\data\mapas\SubSetores_19BPM_estruturado.geojson'
FILE_OUTPUT = r'E:\GitHub\metas_19_2026\public\data\mapas\mapa_interativo_19bpm.html'

def geojson_to_svg():
    if not os.path.exists(FILE_INPUT):
        print(f"Erro: Arquivo {FILE_INPUT} não encontrado!")
        print("Verifique se o caminho está correto ou se o arquivo existe nessa pasta.")
        return

    print(f"Lendo dados de: {FILE_INPUT}...")
    with open(FILE_INPUT, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Encontrar limites (Bounding Box) para o ViewBox
    all_lons = []
    all_lats = []
    
    for feature in data['features']:
        geom = feature['geometry']
        coords = geom['coordinates']
        
        # Lidar com Polygon ou MultiPolygon
        def flatten_coords(c):
            for item in c:
                if isinstance(item[0], (list, tuple)):
                    flatten_coords(item)
                else:
                    all_lons.append(item[0])
                    all_lats.append(item[1])
        flatten_coords(coords)

    if not all_lons:
        print("Erro: Nenhuma coordenada encontrada no GeoJSON.")
        return

    min_x, max_x = min(all_lons), max(all_lons)
    min_y, max_y = min(all_lats), max(all_lats)

    # Definições de escala para o SVG (800px de largura)
    width = 800
    padding = 20
    scale = (width - 2 * padding) / (max_x - min_x)
    height = (max_y - min_y) * scale + 2 * padding

    def transform(lon, lat):
        x = (lon - min_x) * scale + padding
        y = (max_y - lat) * scale + padding # Inverte Y (coordenada de tela)
        return f"{x:.2f},{y:.2f}"

    print("Processando polígonos...")
    svg_paths = ""
    for feature in data['features']:
        nome = feature['properties'].get('municipio', 'DESCONHECIDO').upper()
        # Se for um subsetor, o nome pode vir como "MUNICIPIO - NOME"
        nome_limpo = nome.split('-')[0].strip()
        
        geom = feature['geometry']
        path_d = ""
        
        if geom['type'] == 'Polygon':
            for ring in geom['coordinates']:
                pts = [transform(p[0], p[1]) for p in ring]
                path_d += "M " + " L ".join(pts) + " Z "
        elif geom['type'] == 'MultiPolygon':
            for poly in geom['coordinates']:
                for ring in poly:
                    pts = [transform(p[0], p[1]) for p in ring]
                    path_d += "M " + " L ".join(pts) + " Z "

        svg_paths += f'                <path id="{nome_limpo}" d="{path_d}" />\n'

    html_template = f"""<!DOCTYPE html>
<html lang="pt-br">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Mapa Interativo 19º BPM</title>
    <style>
        body {{ background: #f0f2f5; display: flex; justify-content: center; padding: 20px; margin: 0; }}
        #container-mapa {{ 
            width: 100%; max-width: 850px; background: #fff; padding: 20px;
            box-shadow: 0 10px 25px rgba(0,0,0,0.1); border-radius: 12px; position: relative;
        }}
        h2 {{ text-align: center; color: #2c3e50; font-family: sans-serif; margin-bottom: 20px; }}
        
        /* Estilo do SVG */
        svg {{ width: 100%; height: auto; display: block; }}
        svg path {{
            fill: #34495e !important;
            stroke: #ffffff !important;
            stroke-width: 1;
            transition: all 0.3s ease;
            cursor: pointer;
        }}
        svg path:hover {{
            fill: #f39c12 !important;
            filter: drop-shadow(0px 0px 8px rgba(0,0,0,0.3));
        }}

        /* Tooltip */
        #tooltip {{
            position: absolute; background: rgba(0,0,0,0.85); color: white;
            padding: 10px 15px; border-radius: 6px; font-family: sans-serif;
            font-size: 14px; pointer-events: none; display: none; z-index: 1000;
            box-shadow: 0 4px 15px rgba(0,0,0,0.2); border: 1px solid #f39c12;
        }}
    </style>
</head>
<body>

<div id="container-mapa">
    <h2>Área de Atuação - 19º BPM</h2>
    <div id="tooltip"></div>
    <div id="mapa">
        <svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">
            <g id="municipios">
{svg_paths}
            </g>
        </svg>
    </div>
</div>

<script>
    const links = {json.dumps(LINKS_MUNICIPIOS, indent=8)};
    const tooltip = document.getElementById('tooltip');
    const container = document.getElementById('container-mapa');
    const paths = document.querySelectorAll('svg path');

    paths.forEach(el => {{
        const id = el.id.toUpperCase();
        
        el.addEventListener('mouseenter', (e) => {{
            tooltip.innerHTML = `<strong>${{id}}</strong><br><small>Clique para acessar a página</small>`;
            tooltip.style.display = 'block';
        }});

        el.addEventListener('mousemove', (e) => {{
            const rect = container.getBoundingClientRect();
            tooltip.style.left = (e.clientX - rect.left + 15) + 'px';
            tooltip.style.top = (e.clientY - rect.top + 15) + 'px';
        }});

        el.addEventListener('mouseleave', () => tooltip.style.display = 'none');

        el.addEventListener('click', () => {{
            if(links[id]) {{
                const url = `app/institucional/externo/conteudo.action?conteudo=${{links[id]}}&tipoConteudo=subP`;
                window.open(url, '_top');
            }} else {{
                console.log("Link não configurado para:", id);
            }}
        }});
    }});
</script>

</body>
</html>
"""
    with open(FILE_OUTPUT, 'w', encoding='utf-8') as f:
        f.write(html_template)
    
    print(f"Sucesso! O arquivo '{FILE_OUTPUT}' foi gerado.")

if __name__ == "__main__":
    geojson_to_svg()