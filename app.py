import os
import json
import requests
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS # Não precisa de cross_origin aqui se usar CORS(app) globalmente
from datetime import datetime, timedelta
import traceback
from statistics import mean # Mantenha esta se você a utiliza em cálculos
import base64

# --- 1. DEFINIÇÃO DE VARIÁVEIS DE AMBIENTE GLOBAIS (DEVE ESTAR AQUI) ---
SUPABASE_REST_URL = os.environ.get("SUPABASE_REST_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# --- 2. INICIALIZAÇÃO DO FLASK E CORS (DEVE VIR DEPOIS DAS VARS DE AMBIENTE) ---
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ["https://gestor.thehrkey.tech"]}}, supports_credentials=True)

# --- 3. FUNÇÕES AUXILIARES GLOBAIS (DEVEM VIR DEPOIS DO APP E ANTES DAS ROTAS) ---

# --- FUNÇÃO ÚNICA E GENÉRICA PARA SALVAR JSON NO SUPABASE (relatorios_gerados) ---
def salvar_json_no_supabase(dados_para_salvar, empresa, codrodada, emaillider_val, tipo_do_json):
    """
    Salva um JSON (de relatório ou gráfico) no Supabase, na tabela relatorios_gerados.
    Recebe o JSON a ser salvo e metadados para identificação.
    """
    if not SUPABASE_REST_URL or not SUPABASE_KEY:
        print("❌ Não foi possível salvar no Supabase: Variáveis de ambiente não configuradas.")
        return False

    url_tabela = f"{SUPABASE_REST_URL}/relatorios_gerados" # Salva na tabela genérica de relatórios/gráficos

    headers = {
        "Content-Type": "application/json",
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}" # Use a chave de serviço para escrita se for o caso
    }

    payload = {
        "empresa": empresa,
        "codrodada": codrodada,
        "emaillider": emaillider_val,
        "tipo_relatorio": tipo_do_json, # Usa o parâmetro para diferenciar o tipo de JSON
        "dados_json": dados_para_salvar, # O JSON a ser salvo
        "data_criacao": datetime.now().isoformat()
    }

    try:
        response = requests.post(url_tabela, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        print(f"✅ JSON do tipo '{tipo_do_json}' salvo no Supabase com sucesso.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao salvar JSON do tipo '{tipo_do_json}' no Supabase: {e}")
        if hasattr(response, 'status_code') and hasattr(response, 'text'):
            print(f"Detalhes da resposta do Supabase: Status {response.status_code} - {response.text}")
        else:
            print("Não foi possível obter detalhes da resposta do Supabase.")
        return False

# --- 4. CARREGAMENTO DE PLANILHAS GLOBAIS (DEVE VIR DEPOIS DAS FUNÇÕES AUXILIARES) ---
# Seus carregamentos de planilha de microambiente vêm aqui.
# Certifique-se de que os nomes das variáveis globais estejam consistentes.
try:
    TABELA_DIMENSAO_MICROAMBIENTE_DF = pd.read_excel("pontos_maximos_dimensao.xlsx")
    print("DEBUG: pontos_maximos_dimensao.xlsx carregada com sucesso.")
except FileNotFoundError:
    print("ERRO CRÍTICO: Arquivo 'pontos_maximos_dimensao.xlsx' não encontrado. A aplicação pode não funcionar corretamente.")
    TABELA_DIMENSAO_MICROAMBIENTE_DF = pd.DataFrame()
except Exception as e:
    print(f"ERRO CRÍTICO: Ao carregar 'pontos_maximos_dimensao.xlsx': {str(e)}.")
    TABELA_DIMENSAO_MICROAMBIENTE_DF = pd.DataFrame()

try:
    TABELA_SUBDIMENSAO_MICROAMBIENTE_DF = pd.read_excel("pontos_maximos_subdimensao.xlsx")
    print("DEBUG: pontos_maximos_subdimensao.xlsx carregada com sucesso.")
except FileNotFoundError:
    print("ERRO CRÍTICO: Arquivo 'pontos_maximos_subdimensao.xlsx' não encontrado. A aplicação pode não funcionar corretamente.")
    TABELA_SUBDIMENSAO_MICROAMBIENTE_DF = pd.DataFrame()
except Exception as e:
    print(f"ERRO CRÍTICO: Ao carregar 'pontos_maximos_subdimensao.xlsx': {str(e)}.")
    TABELA_SUBDIMENSAO_MICROAMBIENTE_DF = pd.DataFrame()

try:
    MATRIZ_MICROAMBIENTE_DF = pd.read_excel(
        "TABELA_GERAL_MICROAMBIENTE_COM_CHAVE.xlsx",
        dtype={"PONTUACAO_IDEAL": float, "PONTUACAO_REAL": float} # Ajuste conforme suas colunas
    )
    print("DEBUG: TABELA_GERAL_MICROAMBIENTE_COM_CHAVE.xlsx carregada com sucesso.")
except FileNotFoundError:
    print("ERRO CRÍTICO: Arquivo 'TABELA_GERAL_MICROAMBIENTE_COM_CHAVE.xlsx' não encontrado. A aplicação pode não funcionar corretamente.")
    MATRIZ_MICROAMBIENTE_DF = pd.DataFrame()
except Exception as e:
    print(f"ERRO CRÍTICO: Ao carregar 'TABELA_GERAL_MICROAMBIENTE_COM_CHAVE.xlsx': {str(e)}.")
    MATRIZ_MICROAMBIENTE_DF = pd.DataFrame()


# --- 5. DEFINIÇÕES DE ROTAS COMEÇAM A PARTIR DAQUI ---
# Por exemplo, sua rota home:
@app.route("/")
def home():
    return "API Microambiente Online"

# ... (suas outras rotas virão aqui, incluindo a salvar-grafico-media-equipe-dimensao) ...

# --- EXECUÇÃO DO FLASK APP ---
# Este bloco deve estar no final do seu app.py, após todas as rotas.
# if __name__ == "__main__":
#     app.run(host='0.0.0.0', port=os.environ.get('PORT', 5000))

# @app.route("/")
# def home():
#    return "API Microambiente Online"

@app.route("/avaliar", methods=["POST"])
def avaliar():
    dados = request.get_json()
    if not dados:
        return jsonify({"erro": "Nenhum dado recebido"}), 400

    respostas = {k: v for k, v in dados.items() if k.startswith("Q")}
    acumulado = {}

    for chave, valor in respostas.items():
        try:
            nota = int(valor)
            if nota < 1 or nota > 6:
                continue
        except:
            continue

        tipo = "I1" if "_ideal" in chave.lower() else "R1"
        cod = chave.replace("_ideal", "").replace("_real", "").replace("_IDEAL", "").replace("_REAL", "")
        chave_matriz = f"{cod}_{tipo}_R{nota}"

        linha = matriz[matriz["CHAVE"] == chave_matriz]
        if linha.empty:
            continue

        dim = linha.iloc[0]["DIMENSAO"]
        sub = linha.iloc[0]["SUBDIMENSAO"]
        pontos = 100

        acumulado.setdefault(sub, {}).setdefault(tipo, 0)
        acumulado[sub][tipo] += pontos

    resultado_sub = {}
    for _, row in tabela_sub.iterrows():
        sub = row["SUBDIMENSAO"]
        max_pontos = row["PONTOS_MAXIMOS_SUBDIMENSAO"]
        ideal = acumulado.get(sub, {}).get("I1", 0)
        real = acumulado.get(sub, {}).get("R1", 0)
        resultado_sub[sub] = {
            "ideal": round((ideal / max_pontos) * 100, 1) if max_pontos else 0,
            "real": round((real / max_pontos) * 100, 1) if max_pontos else 0
        }

    resultado_dim = {}
    for _, row in tabela_dim.iterrows():
        dim = row["DIMENSAO"]
        subdimensoes = tabela_sub[tabela_sub["DIMENSAO"] == dim]["SUBDIMENSAO"]
        total_ideal = sum(acumulado.get(sub, {}).get("I1", 0) for sub in subdimensoes)
        total_real = sum(acumulado.get(sub, {}).get("R1", 0) for sub in subdimensoes)
        max_dim = row["PONTOS_MAXIMOS_DIMENSAO"]
        resultado_dim[dim] = {
            "ideal": round((total_ideal / max_dim) * 100, 1) if max_dim else 0,
            "real": round((total_real / max_dim) * 100, 1) if max_dim else 0
        }

    return jsonify({
        "dimensoes": resultado_dim,
        "subdimensoes": resultado_sub
    })

@app.route("/enviar-avaliacao", methods=["POST", "OPTIONS"])

def enviar_avaliacao():
    if request.method == "OPTIONS":
        return '', 200

    import datetime
    import requests

    dados = request.get_json()
    if not dados:
        return jsonify({"erro": "Nenhum dado recebido"}), 400

    print("✅ Dados recebidos:", dados)

    try:
        # Dados principais
        empresa = dados.get("empresa", "").strip().lower()
        codrodada = dados.get("codrodada", "").strip().lower()
        emailLider = dados.get("emailLider", "").strip().lower()
        tipo = dados.get("tipo", "").strip().lower()

        if not all([empresa, codrodada, emailLider, tipo]):
            return jsonify({"erro": "Campos obrigatórios ausentes."}), 400

        # Supabase: URL e headers
        url_supabase = "https://xmsjjknpnowsswwrbvpc.supabase.co/rest/v1/relatorios_microambiente"

        headers = {
            "apikey": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inhtc2pqa25wbm93c3N3d3JidnBjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTI1MDg0NDUsImV4cCI6MjA2ODA4NDQ0NX0.OexXJX7lK_DefGb72VDWGLDcUXamoQIgYOv5Zo_e9L4",
            "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inhtc2pqa25wbm93c3N3d3JidnBjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTI1MDg0NDUsImV4cCI6MjA2ODA4NDQ0NX0.OexXJX7lK_DefGb72VDWGLDcUXamoQIgYOv5Zo_e9L4",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }

        # Payload para Supabase
        registro = { 
            "empresa": empresa,
            "codrodada": codrodada,
            "emailLider": emailLider,
            "tipo": tipo,
            "nome": dados.get("nome", "").strip(),
            "email": dados.get("email", "").strip().lower(),
            "nomeLider": dados.get("nomeLider", "").strip(),
            "departamento": dados.get("departamento", "").strip(),
            "estado": dados.get("estado", "").strip(),
            "nascimento": dados.get("nascimento", "").strip(),
            "sexo": dados.get("sexo", "").strip().lower(),
            "etnia": dados.get("etnia", "").strip().lower(),
            "data": dados.get("data", "").strip(),  # data do preenchimento
            "cargo": dados.get("cargo", "").strip(),
            "area": dados.get("area", "").strip(),
            "cidade": dados.get("cidade", "").strip(),
            "pais": dados.get("pais", "").strip(),
            "data_criacao": datetime.datetime.now().isoformat(),
            "dados_json": dados  # backup completo
        }



        # Envio para Supabase
        print("📦 Registro sendo enviado ao Supabase:")
        print(json.dumps(registro, indent=2, ensure_ascii=False))

        resposta = requests.post(url_supabase, headers=headers, json=registro)
        

        
        if resposta.status_code == 201:
            print("✅ Avaliação salva no Supabase com sucesso!")
            return jsonify({"status": "✅ Microambiente de Equipes → salvo no banco de dados"}), 200
        else:
            print("❌ Erro Supabase:", resposta.status_code)
            try:
                print("❌ Corpo da resposta:", resposta.json())
            except:
                print("❌ Corpo da resposta (raw):", resposta.text)
            return jsonify({"erro": resposta.text}), 500

    except Exception as e:
        print("❌ Erro ao processar dados:", str(e))
        return jsonify({"erro": str(e)}), 500



@app.route("/grafico-autoavaliacao", methods=["POST"])
def grafico_autoavaliacao():
    from datetime import datetime
    import matplotlib.pyplot as plt
    import pandas as pd
    import json
    import os

    try:
        # JSON enviado via POST
        arquivo = request.files.get("arquivo_json")
        if not arquivo:
            return jsonify({"erro": "Arquivo JSON não enviado"}), 400

        # Carregar planilhas auxiliares (já devem estar no diretório)
        matriz = pd.read_excel("TABELA_GERAL_MICROAMBIENTE_COM_CHAVE.xlsx")
        pontos_maximos = pd.read_excel("pontos_maximos_dimensao.xlsx")

        # Parse do JSON
        dados_json = json.load(arquivo)
        auto = dados_json.get("autoavaliacao")
        if not auto:
            return jsonify({"erro": "Bloco 'autoavaliacao' não encontrado"}), 400

        pontos_por_dimensao = {}

        for i in range(1, 49):
            q = f"Q{i:02d}"
            real = int(auto.get(f"{q}C", 0))
            ideal = int(auto.get(f"{q}k", 0))

            chave = f"{q}_I{ideal}_R{real}"
            linha = matriz[matriz["CHAVE"] == chave]
            if linha.empty:
                continue

            dim = linha.iloc[0]["DIMENSAO"]
            pontos_ideal = linha.iloc[0]["PONTUACAO_IDEAL"]
            pontos_real = linha.iloc[0]["PONTUACAO_REAL"]

            if dim not in pontos_por_dimensao:
                pontos_por_dimensao[dim] = {"ideal": 0, "real": 0}

            pontos_por_dimensao[dim]["ideal"] += pontos_ideal
            pontos_por_dimensao[dim]["real"] += pontos_real

        # Calcular % com base nos pontos máximos
        porcentagens = {}
        for _, row in pontos_maximos.iterrows():
            dim = row["DIMENSAO"]
            max_pontos = row["PONTOS_MAXIMOS_DIMENSAO"]
            total = pontos_por_dimensao.get(dim, {"ideal": 0, "real": 0})
            porcentagens[dim] = {
                "ideal": round((total["ideal"] / max_pontos) * 100, 1),
                "real": round((total["real"] / max_pontos) * 100, 1)
            }

        # Criar gráfico
        labels = list(porcentagens.keys())
        valores_ideal = [porcentagens[d]["ideal"] for d in labels]
        valores_real = [porcentagens[d]["real"] for d in labels]

        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(labels, valores_real, marker="o", label="Como é", color="navy")
        ax.plot(labels, valores_ideal, marker="o", label="Como deveria ser", color="darkorange")
        ax.axhline(60, color="gray", linestyle="--", linewidth=1)
        ax.set_ylim(0, 100)
        ax.set_yticks(range(0, 101, 10))
        ax.set_ylabel("% de Engajamento")
        ax.set_title("MICROAMBIENTE DE EQUIPES – DIMENSÕES", fontsize=16, weight="bold")
        ax.set_facecolor("#f2f2f2")

        # Subtítulo
        empresa = auto.get("empresa", "Empresa")
        emailLider = auto.get("emailLider", "email")
        codrodada = auto.get("codrodada", "")
        
        data_hora = datetime.now().strftime("%d/%m/%Y %H:%M")
        
        plt.text(0.5, -0.18, subtitulo, ha="center", va="top", transform=ax.transAxes, fontsize=10)

        ax.legend()
        ax.set_xticklabels(x, rotation=45, ha="right", fontsize=9)


        plt.tight_layout()

        nome_arquivo = "grafico_dimensoes_autoavaliacao.png"
        plt.savefig(nome_arquivo)

        return jsonify({"status": "✅ Gráfico gerado com sucesso", "arquivo": nome_arquivo}), 200

    except Exception as e:
        return jsonify({"erro": str(e)}), 500



@app.route("/graficos-autoavaliacao", methods=["OPTIONS"])
def preflight_graficos_autoavaliacao():
    return '', 200


# ROTA PARA GERAR E SALVAR GRÁFICO DE AUTOAVALIAÇÃO DE MICROAMBIENTE
@app.route("/salvar-grafico-autoavaliacao", methods=["POST", "OPTIONS"])
def salvar_grafico_autoavaliacao():
    if request.method == "OPTIONS":
        response = jsonify({'status': 'CORS preflight OK'})
        response.headers["Access-Control-Allow-Origin"] = "https://gestor.thehrkey.tech"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
        response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
        return response

    try:
        from statistics import mean
        import requests
        from datetime import datetime, timedelta
        from pandas import DataFrame, to_numeric

        dados = request.get_json()
        empresa = dados.get("empresa")
        codrodada = dados.get("codrodada")
        emaillider_req = dados.get("emailLider")

        if not all([empresa, codrodada, emaillider_req]):
            return jsonify({"erro": "Campos obrigatórios ausentes."}), 400

        tipo_relatorio_grafico_atual = "microambiente_grafico_autoavaliacao_dimensao"

        url_busca_cache = f"{SUPABASE_REST_URL}/relatorios_gerados"
        headers_cache_busca = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }
        params_cache = {
            "empresa": f"eq.{empresa}",
            "codrodada": f"eq.{codrodada}",
            "emaillider": f"eq.{emaillider_req}",
            "tipo_relatorio": f"eq.{tipo_relatorio_grafico_atual}",
            "order": "data_criacao.desc",
            "limit": 1
        }

        print(f"DEBUG: Buscando cache do gráfico '{tipo_relatorio_grafico_atual}' no Supabase...")
        cache_response = requests.get(url_busca_cache, headers=headers_cache_busca, params=params_cache, timeout=15)
        cache_response.raise_for_status()
        cached_data_list = cache_response.json()

        if cached_data_list:
            cached_report = cached_data_list[0]
            data_criacao_cache_str = cached_report.get("data_criacao")
            if data_criacao_cache_str:
                data_criacao_cache = datetime.fromisoformat(data_criacao_cache_str.replace('Z', '+00:00'))
                if datetime.now(data_criacao_cache.tzinfo) - data_criacao_cache < timedelta(hours=1):
                    print("✅ Cache válido encontrado. Retornando dados cacheados.")
                    return jsonify(cached_report.get("dados_json", {})), 200

        url_consolidado = f"{SUPABASE_REST_URL}/consolidado_microambiente"
        headers_consolidado = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }
        params_consolidado = {
            "empresa": f"eq.{empresa}",
            "codrodada": f"eq.{codrodada}",
            "emaillider": f"eq.{emaillider_req}"
        }

        print(f"DEBUG: Buscando consolidado microambiente para {empresa}, rodada {codrodada}, líder {emaillider_req}")
        response = requests.get(url_consolidado, headers=headers_consolidado, params=params_consolidado, timeout=30)
        response.raise_for_status()
        data_list = response.json()
        if not data_list:
            return jsonify({"erro": "Consolidado não encontrado."}), 404

        dados_consolidado = data_list[-1].get("dados_json", {})
        respostas_auto = dados_consolidado.get("autoavaliacao", {})

        matriz = MATRIZ_MICROAMBIENTE_DF
        pontos_dim = TABELA_DIMENSAO_MICROAMBIENTE_DF

        calculo = []
        for i in range(1, 49):
            q = f"Q{i:02d}"
            q_real = f"{q}C"
            q_ideal = f"{q}k"

            val_real_str = respostas_auto.get(q_real)
            val_ideal_str = respostas_auto.get(q_ideal)

            valor_real = int(val_real_str) if val_real_str and val_real_str.strip().isdigit() else 0
            valor_ideal = int(val_ideal_str) if val_ideal_str and val_ideal_str.strip().isdigit() else 0

            chave = f"{q}_I{valor_ideal}_R{valor_real}"
            linha = matriz[matriz["CHAVE"] == chave]

            if not linha.empty:
                dim = linha.iloc[0]["DIMENSAO"]
                pi = float(linha.iloc[0]["PONTUACAO_IDEAL"])
                pr = float(linha.iloc[0]["PONTUACAO_REAL"])
                calculo.append((dim, pi, pr))

        df = DataFrame(calculo, columns=["DIMENSAO", "IDEAL", "REAL"])
        df["IDEAL"] = to_numeric(df["IDEAL"], errors="coerce").fillna(0)
        df["REAL"] = to_numeric(df["REAL"], errors="coerce").fillna(0)

        resultado = df.groupby("DIMENSAO").sum().reset_index()
        resultado = resultado.merge(pontos_dim, on="DIMENSAO")
        resultado["PONTOS_MAXIMOS_DIMENSAO"] = to_numeric(resultado["PONTOS_MAXIMOS_DIMENSAO"], errors="coerce").fillna(0)
        resultado["IDEAL_%"] = (resultado["IDEAL"] / resultado["PONTOS_MAXIMOS_DIMENSAO"] * 100).round(1)
        resultado["REAL_%"] = (resultado["REAL"] / resultado["PONTOS_MAXIMOS_DIMENSAO"] * 100).round(1)

        data_hora = datetime.now().strftime("%d/%m/%Y %H:%M")

        dados_json = {
            "titulo": "AUTOAVALIAÇÃO - DIMENSÕES",
            "subtitulo": f"{empresa} / {emaillider_req} / {codrodada} / {data_hora}",
            "info_avaliacoes": "Autoavaliação do Líder",
            "dados": resultado[["DIMENSAO", "IDEAL_%", "REAL_%"]].to_dict(orient="records")
        }

        salvar_json_no_supabase(dados_json, empresa, codrodada, emaillider_req, tipo_relatorio_grafico_atual)
        return jsonify(dados_json), 200

    except Exception as e:
        import traceback
        print("\n" + "="*60)
        print("🚨 ERRO CRÍTICO NA ROTA salvar-grafico-autoavaliacao")
        print(f"Tipo: {type(e).__name__}")
        print(f"Mensagem: {str(e)}")
        traceback.print_exc()
        print("="*60 + "\n")
        return jsonify({"erro": str(e), "debug_info": "Verifique os logs para detalhes."}), 500


@app.route("/salvar-grafico-autoavaliacao-subdimensao", methods=["POST", "OPTIONS"])
def salvar_grafico_autoavaliacao_subdimensao():
    if request.method == "OPTIONS":
        response = jsonify({'status': 'CORS preflight OK'})
        response.headers["Access-Control-Allow-Origin"] = "https://gestor.thehrkey.tech"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
        response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
        return response

    try:
        from statistics import mean
        import requests
        from datetime import datetime, timedelta
        from pandas import DataFrame, to_numeric

        dados = request.get_json()
        empresa = dados.get("empresa")
        codrodada = dados.get("codrodada")
        emaillider_req = dados.get("emailLider")

        if not all([empresa, codrodada, emaillider_req]):
            return jsonify({"erro": "Campos obrigatórios ausentes."}), 400

        tipo_relatorio_grafico_atual = "microambiente_grafico_autoavaliacao_subdimensao"

        url_busca_cache = f"{SUPABASE_REST_URL}/relatorios_gerados"
        headers_cache_busca = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }
        params_cache = {
            "empresa": f"eq.{empresa}",
            "codrodada": f"eq.{codrodada}",
            "emaillider": f"eq.{emaillider_req}",
            "tipo_relatorio": f"eq.{tipo_relatorio_grafico_atual}",
            "order": "data_criacao.desc",
            "limit": 1
        }

        print(f"DEBUG: Buscando cache do gráfico '{tipo_relatorio_grafico_atual}' no Supabase...")
        cache_response = requests.get(url_busca_cache, headers=headers_cache_busca, params=params_cache, timeout=15)
        cache_response.raise_for_status()
        cached_data_list = cache_response.json()

        if cached_data_list:
            cached_report = cached_data_list[0]
            data_criacao_cache_str = cached_report.get("data_criacao")
            if data_criacao_cache_str:
                data_criacao_cache = datetime.fromisoformat(data_criacao_cache_str.replace('Z', '+00:00'))
                if datetime.now(data_criacao_cache.tzinfo) - data_criacao_cache < timedelta(hours=1):
                    print("✅ Cache válido encontrado. Retornando dados cacheados.")
                    return jsonify(cached_report.get("dados_json", {})), 200

        # Buscar consolidado
        url_consolidado = f"{SUPABASE_REST_URL}/consolidado_microambiente"
        headers_consolidado = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }
        params_consolidado = {
            "empresa": f"eq.{empresa}",
            "codrodada": f"eq.{codrodada}",
            "emaillider": f"eq.{emaillider_req}"
        }
        print(f"DEBUG: Buscando consolidado microambiente para {empresa}, rodada {codrodada}, líder {emaillider_req}")
        response = requests.get(url_consolidado, headers=headers_consolidado, params=params_consolidado, timeout=30)
        response.raise_for_status()
        data_list = response.json()
        if not data_list:
            return jsonify({"erro": "Consolidado não encontrado."}), 404

        dados_consolidado = data_list[-1].get("dados_json", {})
        respostas_auto = dados_consolidado.get("autoavaliacao", {})

        matriz = MATRIZ_MICROAMBIENTE_DF
        pontos_sub = TABELA_SUBDIMENSAO_MICROAMBIENTE_DF

        calculo = []
        for i in range(1, 49):
            q = f"Q{i:02d}"
            q_real = f"{q}C"
            q_ideal = f"{q}k"

            val_real_str = respostas_auto.get(q_real)
            val_ideal_str = respostas_auto.get(q_ideal)

            valor_real = int(val_real_str) if val_real_str and val_real_str.strip().isdigit() else 0
            valor_ideal = int(val_ideal_str) if val_ideal_str and val_ideal_str.strip().isdigit() else 0

            chave = f"{q}_I{valor_ideal}_R{valor_real}"
            linha = matriz[matriz["CHAVE"] == chave]

            if not linha.empty:
                subdim = linha.iloc[0]["SUBDIMENSAO"]
                pi = float(linha.iloc[0]["PONTUACAO_IDEAL"])
                pr = float(linha.iloc[0]["PONTUACAO_REAL"])
                calculo.append((subdim, pi, pr))

        df = DataFrame(calculo, columns=["SUBDIMENSAO", "IDEAL", "REAL"])
        df["IDEAL"] = to_numeric(df["IDEAL"], errors="coerce").fillna(0)
        df["REAL"] = to_numeric(df["REAL"], errors="coerce").fillna(0)

        resultado = df.groupby("SUBDIMENSAO").sum().reset_index()
        resultado = resultado.merge(pontos_sub, on="SUBDIMENSAO")
        resultado["PONTOS_MAXIMOS_SUBDIMENSAO"] = to_numeric(resultado["PONTOS_MAXIMOS_SUBDIMENSAO"], errors="coerce").fillna(0)
        resultado["IDEAL_%"] = (resultado["IDEAL"] / resultado["PONTOS_MAXIMOS_SUBDIMENSAO"] * 100).round(1)
        resultado["REAL_%"] = (resultado["REAL"] / resultado["PONTOS_MAXIMOS_SUBDIMENSAO"] * 100).round(1)

        data_hora = datetime.now().strftime("%d/%m/%Y %H:%M")

        dados_json = {
            "titulo": "AUTOAVALIAÇÃO - SUBDIMENSÕES",
            "subtitulo": f"{empresa} / {emaillider_req} / {codrodada} / {data_hora}",
            "info_avaliacoes": "Autoavaliação do Líder",
            "dados": resultado[["SUBDIMENSAO", "IDEAL_%", "REAL_%"]].to_dict(orient="records")
        }

        salvar_json_no_supabase(dados_json, empresa, codrodada, emaillider_req, tipo_relatorio_grafico_atual)
        return jsonify(dados_json), 200

    except Exception as e:
        import traceback
        print("\n" + "="*60)
        print("🚨 ERRO CRÍTICO NA ROTA salvar-grafico-autoavaliacao-subdimensao")
        print(f"Tipo: {type(e).__name__}")
        print(f"Mensagem: {str(e)}")
        traceback.print_exc()
        print("="*60 + "\n")
        return jsonify({"erro": str(e), "debug_info": "Verifique os logs para detalhes."}), 500


@app.route("/salvar-grafico-media-equipe-dimensao", methods=["POST", "OPTIONS"])
def salvar_grafico_media_equipe_dimensao():
    if request.method == "OPTIONS":
        response = jsonify({'status': 'CORS preflight OK'})
        response.headers["Access-Control-Allow-Origin"] = "https://gestor.thehrkey.tech"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
        response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
        return response

    try:
        from statistics import mean
        import requests
        from datetime import datetime
        
        # As variáveis SUPABASE_REST_URL e SUPABASE_KEY são globais, não precisam ser redefinidas aqui.
        
        dados = request.get_json()
        empresa = dados.get("empresa")
        codrodada = dados.get("codrodada")
        emaillider_req = dados.get("emailLider") # Consistente com o frontend

        if not all([empresa, codrodada, emaillider_req]):
            return jsonify({"erro": "Campos obrigatórios ausentes."}), 400

        # --- Lógica de Caching: Buscar JSON do Gráfico Salvo ---
        tipo_relatorio_grafico_atual = "microambiente_grafico_mediaequipe_dimensao" 

        url_busca_cache = f"{SUPABASE_REST_URL}/relatorios_gerados"

        headers_cache_busca = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }

        params_cache = {
            "empresa": f"eq.{empresa}",
            "codrodada": f"eq.{codrodada}",
            "emaillider": f"eq.{emaillider_req}", 
            "tipo_relatorio": f"eq.{tipo_relatorio_grafico_atual}",
            "order": "data_criacao.desc",
            "limit": 1
        }

        print(f"DEBUG: Buscando cache do gráfico '{tipo_relatorio_grafico_atual}' no Supabase...")
        cache_response = requests.get(url_busca_cache, headers=headers_cache_busca, params=params_cache, timeout=15)
        cache_response.raise_for_status()
        cached_data_list = cache_response.json()

        if cached_data_list:
            cached_report = cached_data_list[0]
            data_criacao_cache_str = cached_report.get("data_criacao")
            
            if data_criacao_cache_str:
                data_criacao_cache = datetime.fromisoformat(data_criacao_cache_str.replace('Z', '+00:00')) 
                cache_validity_period = timedelta(hours=1) # Cache válido por 1 hora

                if datetime.now(data_criacao_cache.tzinfo) - data_criacao_cache < cache_validity_period:
                    print(f"✅ Cache válido encontrado para o gráfico '{tipo_relatorio_grafico_atual}'. Retornando dados cacheados.")
                    return jsonify(cached_report.get("dados_json", {})), 200
                else:
                    print(f"Cache do gráfico '{tipo_relatorio_grafico_atual}' expirado. Recalculando...")
            else:
                print("Cache encontrado, mas sem data de criação válida. Recalculando...")
        else:
            print(f"Cache do gráfico '{tipo_relatorio_grafico_atual}' não encontrado. Recalculando...")

        # --- BUSCAR RELATÓRIO CONSOLIDADO DE MICROAMBIENTE DO SUPABASE ---
        url_consolidado_microambiente = f"{SUPABASE_REST_URL}/consolidado_microambiente" # Usando GLOBAL SUPABASE_REST_URL
        
        params_consolidado = {
            "empresa": f"eq.{empresa}",
            "codrodada": f"eq.{codrodada}",
            "emaillider": f"eq.{emaillider_req}"
        }

        print(f"DEBUG: Buscando consolidado de microambiente no Supabase para Empresa: {empresa}, Rodada: {codrodada}, Líder: {emaillider_req}")
        
        headers_consolidado_busca = {
            "apikey": SUPABASE_KEY, # Usando GLOBAL SUPABASE_KEY
            "Authorization": f"Bearer {SUPABASE_KEY}" # Usando GLOBAL SUPABASE_KEY
        }

        consolidado_response = requests.get(url_consolidado_microambiente, headers=headers_consolidado_busca, params=params_consolidado, timeout=30)
        consolidado_response.raise_for_status() # Lança erro para status HTTP ruins

        consolidated_data_list = consolidado_response.json()

        if not consolidated_data_list:
            return jsonify({"erro": "Consolidado de microambiente não encontrado no Supabase para os dados fornecidos."}), 404

        microambiente_consolidado = consolidated_data_list[-1] 

        dados_do_consolidado = microambiente_consolidado.get("dados_json", {})
        
        # Extrair respostas para autoavaliação e equipe do JSON consolidado ANINHADO
        respostas_auto = dados_do_consolidado.get("autoavaliacao", {})
        avaliacoes = dados_do_consolidado.get("avaliacoesEquipe", []) # Variável 'avaliacoes' para o loop de cálculo


        print(f"DEBUG: Conteúdo de microambiente_consolidado (após fetch): {microambiente_consolidado}")
        print(f"DEBUG: Conteúdo de respostas_auto: {respostas_auto}")
        print(f"DEBUG: Conteúdo de avaliacoes (equipe): {avaliacoes}")
        print(f"DEBUG: len(avaliacoes): {len(avaliacoes)}")
        
        # --- CARREGAR MATRIZES LOCAIS (já estão globais, usar as vars globais) ---
        matriz = MATRIZ_MICROAMBIENTE_DF # Usando a variável global
        pontos_dim = TABELA_DIMENSAO_MICROAMBIENTE_DF # Usando a variável global

        calculo = []
        for i in range(1, 49):
            q = f"Q{i:02d}"
            q_real = f"{q}C"
            q_ideal = f"{q}k"

            # Converte as respostas para INT de forma segura
            val_real_auto_str = respostas_auto.get(q_real)
            valor_real_auto = int(val_real_auto_str) if val_real_auto_str and isinstance(val_real_auto_str, str) and val_real_auto_str.strip().isdigit() else 0

            val_ideal_auto_str = respostas_auto.get(q_ideal)
            valor_ideal_auto = int(val_ideal_auto_str) if val_ideal_auto_str and isinstance(val_ideal_auto_str, str) and val_ideal_auto_str.strip().isdigit() else 0
            
            valores_real_equipe = []
            for av in avaliacoes:
                val_str = av.get(q_real)
                if val_str is not None and isinstance(val_str, str) and val_str.strip().isdigit():
                    valores_real_equipe.append(int(val_str))
            
            valores_ideal_equipe = []
            for av in avaliacoes:
                val_str = av.get(q_ideal)
                if val_str is not None and isinstance(val_str, str) and val_str.strip().isdigit():
                    valores_ideal_equipe.append(int(val_str))

            media_real = round(mean(valores_real_equipe)) if valores_real_equipe else 0
            media_ideal = round(mean(valores_ideal_equipe)) if valores_ideal_equipe else 0
            
            chave = f"{q}_I{media_ideal}_R{media_real}"

            # ... (código dentro do loop 'for i in range(1, 49):') ...

            linha = matriz[matriz["CHAVE"] == chave]
            if not linha.empty:
                dim = linha.iloc[0]["DIMENSAO"]
                pi_val = linha.iloc[0]["PONTUACAO_IDEAL"] # Pega o valor original
                pr_val = linha.iloc[0]["PONTUACAO_REAL"] # Pega o valor original
    
                # --- ADICIONE ESTAS DUAS LINHAS PARA FORÇAR FLOAT ---
                # Garante que pi e pr são floats. errors='coerce' transforma não-números em NaN.
                # .fillna(0) transforma NaN em 0.
                # .item() extrai o valor escalar puro se ele vier como uma Series de um item.
                pi = float(pi_val)
                pr = float(pr_val)
                # --- FIM DA ADIÇÃO ---
    
                calculo.append((dim, pi, pr)) # Usando pi e pr já convertidos para float

        df = pd.DataFrame(calculo, columns=["DIMENSAO", "IDEAL", "REAL"])
        # Converte as colunas IDEAL e REAL para tipo numérico, tratando erros
        df['IDEAL'] = pd.to_numeric(df['IDEAL'], errors='coerce').fillna(0)
        df['REAL'] = pd.to_numeric(df['REAL'], errors='coerce').fillna(0)
        
        resultado = df.groupby("DIMENSAO").sum().reset_index()
        resultado = resultado.merge(pontos_dim, on="DIMENSAO")
        resultado["PONTOS_MAXIMOS_DIMENSAO"] = pd.to_numeric(resultado["PONTOS_MAXIMOS_DIMENSAO"], errors="coerce").fillna(0)

        resultado["IDEAL_%"] = (resultado["IDEAL"] / resultado["PONTOS_MAXIMOS_DIMENSAO"] * 100).round(1)
        resultado["REAL_%"] = (resultado["REAL"] / resultado["PONTOS_MAXIMOS_DIMENSAO"] * 100).round(1)

        data_hora = datetime.now().strftime("%d/%m/%Y %H:%M")
        numero_avaliacoes = len(avaliacoes) # Número de avaliações da equipe

        dados_json = {
            "titulo": "MÉDIA DA EQUIPE - DIMENSÕES",
            "subtitulo": f"{empresa} / {emaillider_req} / {codrodada} / {data_hora}",
            "info_avaliacoes": f"Equipe: {numero_avaliacoes} respondentes", # Adicionado para o frontend
            "dados": resultado[["DIMENSAO", "IDEAL_%", "REAL_%"]].to_dict(orient="records")
        }
        
        # --- Chamar a função para salvar os dados do gráfico gerados no Supabase ---
        tipo_relatorio_grafico_atual = "microambiente_grafico_mediaequipe_dimensao" 
        salvar_json_no_supabase(dados_json, empresa, codrodada, emaillider_req, tipo_relatorio_grafico_atual) # Usando a função renomeada

        # Retornando o JSON completo para o navegador
        return jsonify(dados_json), 200

    except Exception as e:
        detailed_traceback = traceback.format_exc()
        print("\n" + "="*50) # Linha de destaque no log
        print("🚨🚨🚨 ERRO CRÍTICO NA ROTA salvar-grafico-media-equipe-dimensao 🚨🚨🚨")
        print(f"Tipo do erro: {type(e).__name__}")
        print(f"Mensagem do erro: {str(e)}")
        print("TRACEBACK COMPLETO ABAIXO:")
        traceback.print_exc() # Isso imprime diretamente no sys.stderr, que geralmente vai para o log
        print("="*50 + "\n") # Linha de destaque no log
        
        return jsonify({"erro": str(e), "debug_info": "Verifique os logs do Render.com para detalhes."}), 500

@app.route("/salvar-grafico-media-equipe-subdimensao", methods=["POST", "OPTIONS"])
def salvar_grafico_media_equipe_subdimensao():
    if request.method == "OPTIONS":
        response = jsonify({'status': 'CORS preflight OK'})
        response.headers["Access-Control-Allow-Origin"] = "https://gestor.thehrkey.tech"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
        response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
        return response

    try:
        from statistics import mean
        import requests
        from datetime import datetime, timedelta
        
        dados = request.get_json()
        empresa = dados.get("empresa")
        codrodada = dados.get("codrodada")
        emaillider_req = dados.get("emailLider")

        if not all([empresa, codrodada, emaillider_req]):
            return jsonify({"erro": "Campos obrigatórios ausentes."}), 400

        tipo_relatorio_grafico_atual = "microambiente_grafico_mediaequipe_subdimensao"

        url_busca_cache = f"{SUPABASE_REST_URL}/relatorios_gerados"
        headers_cache_busca = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }
        params_cache = {
            "empresa": f"eq.{empresa}",
            "codrodada": f"eq.{codrodada}",
            "emaillider": f"eq.{emaillider_req}",
            "tipo_relatorio": f"eq.{tipo_relatorio_grafico_atual}",
            "order": "data_criacao.desc",
            "limit": 1
        }

        cache_response = requests.get(url_busca_cache, headers=headers_cache_busca, params=params_cache, timeout=15)
        cache_response.raise_for_status()
        cached_data_list = cache_response.json()

        if cached_data_list:
            cached_report = cached_data_list[0]
            data_criacao_cache_str = cached_report.get("data_criacao")
            if data_criacao_cache_str:
                data_criacao_cache = datetime.fromisoformat(data_criacao_cache_str.replace('Z', '+00:00'))
                if datetime.now(data_criacao_cache.tzinfo) - data_criacao_cache < timedelta(hours=1):
                    return jsonify(cached_report.get("dados_json", {})), 200

        url_consolidado_microambiente = f"{SUPABASE_REST_URL}/consolidado_microambiente"
        params_consolidado = {
            "empresa": f"eq.{empresa}",
            "codrodada": f"eq.{codrodada}",
            "emaillider": f"eq.{emaillider_req}"
        }

        headers_consolidado_busca = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }

        consolidado_response = requests.get(url_consolidado_microambiente, headers=headers_consolidado_busca, params=params_consolidado, timeout=30)
        consolidado_response.raise_for_status()
        consolidated_data_list = consolidado_response.json()

        if not consolidated_data_list:
            return jsonify({"erro": "Consolidado não encontrado."}), 404

        microambiente_consolidado = consolidated_data_list[-1]
        dados_do_consolidado = microambiente_consolidado.get("dados_json", {})
        respostas_auto = dados_do_consolidado.get("autoavaliacao", {})
        avaliacoes = dados_do_consolidado.get("avaliacoesEquipe", [])

        matriz = MATRIZ_MICROAMBIENTE_DF
        pontos_subdim = TABELA_SUBDIMENSAO_MICROAMBIENTE_DF  # <- TROCA PRINCIPAL AQUI

        calculo = []
        for i in range(1, 49):
            q = f"Q{i:02d}"
            q_real = f"{q}C"
            q_ideal = f"{q}k"

            val_real_auto = respostas_auto.get(q_real)
            val_ideal_auto = respostas_auto.get(q_ideal)

            valor_real_auto = int(val_real_auto) if val_real_auto and val_real_auto.strip().isdigit() else 0
            valor_ideal_auto = int(val_ideal_auto) if val_ideal_auto and val_ideal_auto.strip().isdigit() else 0

            valores_real_equipe = [int(av.get(q_real)) for av in avaliacoes if av.get(q_real, '').strip().isdigit()]
            valores_ideal_equipe = [int(av.get(q_ideal)) for av in avaliacoes if av.get(q_ideal, '').strip().isdigit()]

            media_real = round(mean(valores_real_equipe)) if valores_real_equipe else 0
            media_ideal = round(mean(valores_ideal_equipe)) if valores_ideal_equipe else 0

            chave = f"{q}_I{media_ideal}_R{media_real}"
            linha = matriz[matriz["CHAVE"] == chave]

            if not linha.empty:
                subdim = linha.iloc[0]["SUBDIMENSAO"]
                pi = float(linha.iloc[0]["PONTUACAO_IDEAL"])
                pr = float(linha.iloc[0]["PONTUACAO_REAL"])
                calculo.append((subdim, pi, pr))

        df = pd.DataFrame(calculo, columns=["SUBDIMENSAO", "IDEAL", "REAL"])
        df['IDEAL'] = pd.to_numeric(df['IDEAL'], errors='coerce').fillna(0)
        df['REAL'] = pd.to_numeric(df['REAL'], errors='coerce').fillna(0)

        resultado = df.groupby("SUBDIMENSAO").sum().reset_index()
        resultado = resultado.merge(pontos_subdim, on="SUBDIMENSAO")
        resultado["PONTOS_MAXIMOS_SUBDIMENSAO"] = pd.to_numeric(resultado["PONTOS_MAXIMOS_SUBDIMENSAO"], errors="coerce").fillna(0)

        resultado["IDEAL_%"] = (resultado["IDEAL"] / resultado["PONTOS_MAXIMOS_SUBDIMENSAO"] * 100).round(1)
        resultado["REAL_%"] = (resultado["REAL"] / resultado["PONTOS_MAXIMOS_SUBDIMENSAO"] * 100).round(1)

        data_hora = datetime.now().strftime("%d/%m/%Y %H:%M")
        numero_avaliacoes = len(avaliacoes)

        dados_json = {
            "titulo": "MÉDIA DA EQUIPE - SUBDIMENSÕES",
            "subtitulo": f"{empresa} / {emaillider_req} / {codrodada} / {data_hora}",
            "info_avaliacoes": f"Equipe: {numero_avaliacoes} respondentes",
            "dados": resultado[["SUBDIMENSAO", "IDEAL_%", "REAL_%"]].to_dict(orient="records")
        }

        salvar_json_no_supabase(dados_json, empresa, codrodada, emaillider_req, tipo_relatorio_grafico_atual)
        return jsonify(dados_json), 200

    except Exception as e:
        import traceback
        print("Erro:", traceback.format_exc())
        return jsonify({"erro": str(e)}), 500


@app.route("/salvar-grafico-waterfall-gaps", methods=["POST", "OPTIONS"])
def salvar_grafico_waterfall_gaps():
    if request.method == "OPTIONS":
        response = jsonify({'status': 'CORS preflight OK'})
        response.headers["Access-Control-Allow-Origin"] = "https://gestor.thehrkey.tech"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
        response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
        return response

    try:
        import pandas as pd
        import requests
        from datetime import datetime, timedelta

        dados = request.get_json()
        empresa = dados.get("empresa")
        codrodada = dados.get("codrodada")
        emailLider = dados.get("emailLider")

        if not all([empresa, codrodada, emailLider]):
            return jsonify({"erro": "Campos obrigatórios ausentes."}), 400

        tipo_relatorio = "microambiente_waterfall_gaps"

        # --- Buscar cache no Supabase ---
        url_cache = f"{SUPABASE_REST_URL}/relatorios_gerados"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }
        params = {
            "empresa": f"eq.{empresa}",
            "codrodada": f"eq.{codrodada}",
            "emaillider": f"eq.{emailLider}",
            "tipo_relatorio": f"eq.{tipo_relatorio}",
            "order": "data_criacao.desc",
            "limit": 1
        }

        print(f"DEBUG: Buscando cache waterfall gaps...")
        resp = requests.get(url_cache, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        dados_cache = resp.json()

        if dados_cache:
            data_criacao_str = dados_cache[0].get("data_criacao", "")
            if data_criacao_str:
                data_criacao = datetime.fromisoformat(data_criacao_str.replace("Z", "+00:00"))
                if datetime.now(data_criacao.tzinfo) - data_criacao < timedelta(hours=1):
                    print("✅ Cache válido encontrado. Retornando.")
                    return jsonify(dados_cache[0].get("dados_json", {})), 200

        # --- Buscar consolidado no Supabase ---
        url_consolidado = f"{SUPABASE_REST_URL}/consolidado_microambiente"
        params_consolidado = {
            "empresa": f"eq.{empresa}",
            "codrodada": f"eq.{codrodada}",
            "emaillider": f"eq.{emailLider}"
        }

        print(f"DEBUG: Buscando consolidado para {empresa}/{codrodada}/{emailLider}")
        resp = requests.get(url_consolidado, headers=headers, params=params_consolidado, timeout=30)
        resp.raise_for_status()
        dados = resp.json()

        if not dados:
            return jsonify({"erro": "Consolidado não encontrado."}), 404

        consolidado = dados[-1].get("dados_json", {})
        avaliacoes = consolidado.get("avaliacoesEquipe", [])
        if not avaliacoes:
            return jsonify({"erro": "Nenhuma avaliação encontrada."}), 400

        print(f"DEBUG: Total de avaliações equipe: {len(avaliacoes)}")

        # --- Carregar matriz local (global no app) ---
        matriz = MATRIZ_MICROAMBIENTE_DF

        # --- Cálculo dos GAPs ---
        registros = []
        for i in range(1, 49):
            q = f"Q{i:02d}"
            reais = [int(av.get(f"{q}C", 0)) for av in avaliacoes if str(av.get(f"{q}C", "")).isdigit()]
            ideais = [int(av.get(f"{q}k", 0)) for av in avaliacoes if str(av.get(f"{q}k", "")).isdigit()]
            if not reais or not ideais:
                continue

            media_real = round(sum(reais) / len(reais))
            media_ideal = round(sum(ideais) / len(ideais))
            chave = f"{q}_I{media_ideal}_R{media_real}"
            linha = matriz[matriz["CHAVE"] == chave]

            if not linha.empty:
                row = linha.iloc[0]
                registros.append({
                    "QUESTAO": q,
                    "DIMENSAO": row["DIMENSAO"],
                    "SUBDIMENSAO": row["SUBDIMENSAO"],
                    "GAP": row["GAP"]
                })

        df = pd.DataFrame(registros)
        gap_dim = df.groupby("DIMENSAO")["GAP"].mean().reset_index().sort_values("GAP")
        gap_sub = df.groupby("SUBDIMENSAO")["GAP"].mean().reset_index().sort_values("GAP")

        import matplotlib.pyplot as plt
        import seaborn as sns
        import matplotlib.ticker as mticker
        
        fig, (ax1, ax2) = plt.subplots(nrows=2, figsize=(14, 7))  # Altura reduzida
        
        # Função de cor condicional
        def get_gap_colors(gaps):
            return ['#FFA07A' if abs(g) > 20 else '#ADD8E6' for g in gaps]  # Laranja claro / Azul claro
        
        # --- GRÁFICO 1: Dimensões ---
        sns.barplot(x="DIMENSAO", y="GAP", data=gap_dim, palette=get_gap_colors(gap_dim["GAP"]), ax=ax1)
        ax1.set_title("GAP por Dimensão", fontsize=13)
        ax1.set_ylabel("GAP (%)")
        ax1.set_ylim(-100, 0)
        ax1.yaxis.set_major_locator(mticker.MultipleLocator(10))
        ax1.tick_params(axis='x', rotation=45)
        for bar in ax1.patches:
            h = bar.get_height()
            ax1.annotate(f'{h:.1f}%', (bar.get_x() + bar.get_width() / 2, h - 3), ha='center', fontsize=8)
        
        # --- GRÁFICO 2: Subdimensões ---
        sns.barplot(x="SUBDIMENSAO", y="GAP", data=gap_sub, palette=get_gap_colors(gap_sub["GAP"]), ax=ax2)
        ax2.set_title("GAP por Subdimensão", fontsize=13)
        ax2.set_ylabel("GAP (%)")
        ax2.set_ylim(-100, 0)
        ax2.yaxis.set_major_locator(mticker.MultipleLocator(10))
        ax2.tick_params(axis='x', rotation=90)
        for bar in ax2.patches:
            h = bar.get_height()
            ax2.annotate(f'{h:.1f}%', (bar.get_x() + bar.get_width() / 2, h - 3), ha='center', fontsize=7)
        
        # --- Legenda personalizada ---
        fig.legend(["GAP > 20% = Laranja claro", "GAP ≤ 20% = Azul claro"],
                   loc='upper center', ncol=2, fontsize=9, bbox_to_anchor=(0.5, 1.02))
        
        # --- Ajustes e salvamento ---
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        nome_arquivo_png = f"waterfall_gaps_{emailLider}_{codrodada}.png"
        caminho_png = f"/tmp/{nome_arquivo_png}"
        plt.savefig(caminho_png, dpi=300, bbox_inches='tight')


        print("DEBUG: GAP por dimensão:", gap_dim.to_dict(orient="records"))
        print("DEBUG: GAP por subdimensão:", gap_sub.to_dict(orient="records"))

        data_hora = datetime.now().strftime("%d/%m/%Y %H:%M")
        dados_json = {
            "titulo": "GAP MÉDIO POR DIMENSÃO E SUBDIMENSÃO",
            "subtitulo": f"{empresa} / {emailLider} / {codrodada} / {data_hora}",
            "info_avaliacoes": f"Equipe: {len(avaliacoes)} respondentes",
            "dados": {
                "dimensao": gap_dim.to_dict(orient="records"),
                "subdimensao": gap_sub.to_dict(orient="records")
            }
        }

        salvar_json_no_supabase(dados_json, empresa, codrodada, emailLider, tipo_relatorio)

        response = jsonify(dados_json)
        response.headers["Access-Control-Allow-Origin"] = "https://gestor.thehrkey.tech"
        return response, 200

    except Exception as e:
        import traceback
        print("\n" + "="*60)
        print("🚨 ERRO CRÍTICO NA ROTA salvar-grafico-waterfall-gaps")
        print(f"Tipo: {type(e).__name__}")
        print(f"Mensagem: {str(e)}")
        traceback.print_exc()
        print("="*60 + "\n")
        return jsonify({
            "erro": str(e),
            "debug_info": "Verifique os logs para detalhes."
        }), 500



@app.route("/relatorio-gaps-por-questao", methods=["POST"])
def relatorio_gaps_por_questao():
    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    import seaborn as sns
    import json
    import io
    import os
    from flask import request, jsonify
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

    try:
        dados = request.get_json()
        empresa = dados.get("empresa")
        codrodada = dados.get("codrodada")
        emailLider = dados.get("emailLider")

        if not all([empresa, codrodada, emailLider]):
            return jsonify({"erro": "Campos obrigatórios ausentes."}), 400

        # GOOGLE DRIVE
        SCOPES = ['https://www.googleapis.com/auth/drive']
        creds = service_account.Credentials.from_service_account_info(
            json.loads(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")),
            scopes=SCOPES
        )
        service = build('drive', 'v3', credentials=creds)
        PASTA_RAIZ = "1ekQKwPchEN_fO4AK0eyDd_JID5YO3hAF"

        def buscar_id(nome, pai):
            q = f"'{pai}' in parents and name='{nome}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            resp = service.files().list(q=q, fields="files(id)").execute().get("files", [])
            return resp[0]["id"] if resp else None

        id_empresa = buscar_id(empresa.lower(), PASTA_RAIZ)
        id_rodada = buscar_id(codrodada.lower(), id_empresa)
        id_lider = buscar_id(emailLider.lower(), id_rodada)

        if not id_lider:
            return jsonify({"erro": "Pasta do líder não encontrada."}), 404

        arquivos = service.files().list(
            q=f"'{id_lider}' in parents and mimeType='application/json' and trashed = false",
            fields="files(id, name)").execute().get("files", [])

        dados_equipes = []
        for arq in arquivos:
            nome = arq["name"]
            if "microambiente" in nome and emailLider in nome and codrodada in nome:
                req = service.files().get_media(fileId=arq["id"])
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, req)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
                fh.seek(0)
                conteudo = json.load(fh)
                for bloco in conteudo.get("avaliacoesEquipe", []):
                    if bloco.get("tipo") == "microambiente_equipe":
                        dados_equipes.append(bloco)

        if not dados_equipes:
            return jsonify({"erro": "Nenhuma avaliação encontrada."}), 400

        matriz = pd.read_excel("TABELA_GERAL_MICROAMBIENTE_COM_CHAVE.xlsx")

        # Calcular médias por questão
        somas = {}
        for av in dados_equipes:
            for i in range(1, 49):
                q = f"Q{i:02d}"
                ideal = int(av.get(f"{q}k", 0))
                real = int(av.get(f"{q}C", 0))
                if q not in somas:
                    somas[q] = {"ideal": 0, "real": 0}
                somas[q]["ideal"] += ideal
                somas[q]["real"] += real

        num_avaliacoes = len(dados_equipes)
        registros = []

        for i in range(1, 49):
            q = f"Q{i:02d}"
            media_ideal = round(somas[q]["ideal"] / num_avaliacoes)
            media_real = round(somas[q]["real"] / num_avaliacoes)
            chave = f"{q}_I{media_ideal}_R{media_real}"
            linha = matriz[matriz["CHAVE"] == chave]
            if not linha.empty:
                row = linha.iloc[0]
                registros.append({
                    "QUESTAO": q,
                    "AFIRMACAO": row["AFIRMACAO"],
                    "DIMENSAO": row["DIMENSAO"],
                    "SUBDIMENSAO": row["SUBDIMENSAO"],
                    "PONTUACAO_IDEAL": float(row["PONTUACAO_IDEAL"]),
                    "PONTUACAO_REAL": float(row["PONTUACAO_REAL"]),
                    "GAP": float(row["GAP"])
                })

        df = pd.DataFrame(registros)

        # Início do gráfico
        sns.set(style="whitegrid")
        fig, ax = plt.subplots(figsize=(16, 10))

        df_sorted = df.sort_values("GAP")
        cores = df_sorted["GAP"].apply(lambda x: "red" if x < -20 else ("orange" if x < -10 else "blue"))

        bars = ax.barh(df_sorted["AFIRMACAO"], df_sorted["GAP"], color=cores)

        for i, (bar, gap) in enumerate(zip(bars, df_sorted["GAP"])):
            ax.text(bar.get_width() - 3, bar.get_y() + bar.get_height()/2, f'{gap:.1f}%', va='center', ha='right', fontsize=7, color="white")

        ax.set_title("ANÁLISE DE MICROAMBIENTE - OPORTUNIDADES DE DESENVOLVIMENTO", fontsize=14, weight="bold")
        ax.set_xlabel("GAP (%)")
        ax.set_xlim(-100, 0)
        ax.xaxis.set_major_locator(mticker.MultipleLocator(10))
        plt.tight_layout()

        # Subtítulo no rodapé
        fig.text(0.01, 0.01, f"{empresa} / {emailLider} / {codrodada} / {pd.Timestamp.now().strftime('%d/%m/%Y')}", fontsize=8, color="gray")

        nome_arquivo = f"relatorio_gaps_questao_{emailLider}_{codrodada}.pdf"
        caminho_local = f"/tmp/{nome_arquivo}"
        plt.savefig(caminho_local)

        # Upload para o Google Drive
        file_metadata = {"name": nome_arquivo, "parents": [id_lider]}
        media = MediaIoBaseUpload(open(caminho_local, "rb"), mimetype="application/pdf")
        service.files().create(body=file_metadata, media_body=media, fields="id").execute()

        # Salvar também o JSON com prefixo IA_ na subpasta ia_json
        dados_json = {
            "titulo": "ANÁLISE DE MICROAMBIENTE - GAP POR QUESTÃO",
            "subtitulo": f"{empresa} / {emailLider} / {codrodada} / {pd.Timestamp.now().strftime('%d/%m/%Y')}",
            "dados": df[["QUESTAO", "DIMENSAO", "SUBDIMENSAO", "GAP", "AFIRMACAO"]].to_dict(orient="records")
        }
        salvar_json_ia_no_drive(dados_json, nome_arquivo, service, id_lider)

        return jsonify({"mensagem": f"✅ Relatório salvo com sucesso no Google Drive: {nome_arquivo}"}), 200


    except Exception as e:
        return jsonify({"erro": str(e)}), 500


# Código Python completo para gerar o relatório analítico conforme layout aprovado

# Rota ajustada para gerar o Relatório Analítico de Microambiente com layout refinado

@app.route("/relatorio-analitico-microambiente-supabase", methods=["POST", "OPTIONS"])
def relatorio_analitico_microambiente_supabase():
    from flask import request, jsonify
    import pandas as pd
    import json
    from datetime import datetime
    import traceback

    if request.method == "OPTIONS":
        response = jsonify({'status': 'CORS preflight OK'})
        response.headers["Access-Control-Allow-Origin"] = "https://gestor.thehrkey.tech"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
        response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
        return response

    try:
        dados = request.get_json()
        empresa = dados.get("empresa")
        codrodada = dados.get("codrodada")
        emailLider = dados.get("emailLider")

        if not all([empresa, codrodada, emailLider]):
            return jsonify({"erro": "Campos obrigatórios ausentes."}), 400

        # Buscar dados do Supabase
        url_consolidado = f"{SUPABASE_REST_URL}/consolidado_microambiente"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }
        params = {
            "empresa": f"eq.{empresa}",
            "codrodada": f"eq.{codrodada}",
            "emaillider": f"eq.{emailLider}"
        }

        response = requests.get(url_consolidado, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        consolidado = response.json()

        if not consolidado:
            return jsonify({"erro": "Consolidado não encontrado."}), 404

        microambiente = consolidado[-1].get("dados_json", {})
        avaliacoes = microambiente.get("avaliacoesEquipe", [])

        if not avaliacoes:
            return jsonify({"erro": "Nenhuma avaliação encontrada."}), 400

        matriz = MATRIZ_MICROAMBIENTE_DF  # DataFrame global carregado previamente
        num_avaliacoes = len(avaliacoes)

        somas = {}
        for av in avaliacoes:
            for i in range(1, 49):
                q = f"Q{i:02d}"
                ideal = int(av.get(f"{q}k", 0))
                real = int(av.get(f"{q}C", 0))
                if q not in somas:
                    somas[q] = {"ideal": 0, "real": 0}
                somas[q]["ideal"] += ideal
                somas[q]["real"] += real

        registros = []
        for i in range(1, 49):
            q = f"Q{i:02d}"
            media_ideal = round(somas[q]["ideal"] / num_avaliacoes)
            media_real = round(somas[q]["real"] / num_avaliacoes)
            chave = f"{q}_I{media_ideal}_R{media_real}"
            linha = matriz[matriz["CHAVE"] == chave]

            if not linha.empty:
                row = linha.iloc[0]
                registros.append({
                    "QUESTAO": q,
                    "AFIRMACAO": row["AFIRMACAO"],
                    "DIMENSAO": row["DIMENSAO"],
                    "SUBDIMENSAO": row["SUBDIMENSAO"],
                    "PONTUACAO_IDEAL": float(row["PONTUACAO_IDEAL"]),
                    "PONTUACAO_REAL": float(row["PONTUACAO_REAL"]),
                    "GAP": float(row["GAP"])
                })

        dados_json = {
            "titulo": "RELATÓRIO ANALÍTICO DE MICROAMBIENTE",
            "subtitulo": f"{empresa} / {emailLider} / {codrodada} / {datetime.now().strftime('%d/%m/%Y')}",
            "numeroAvaliacoes": num_avaliacoes,
            "dados": registros
        }

        salvar_json_no_supabase(dados_json, empresa, codrodada, emailLider, "microambiente_analitico")

        return jsonify(dados_json), 200

    except Exception as e:
        print("\n" + "="*60)
        print("🚨 ERRO CRÍTICO NA ROTA relatorio-analitico-microambiente-supabase")
        print(f"Tipo: {type(e).__name__}")
        print(f"Mensagem: {str(e)}")
        traceback.print_exc()
        print("="*60 + "\n")
        return jsonify({"erro": str(e), "debug_info": "Verifique os logs."}), 500



@app.route("/salvar-grafico-termometro-gaps", methods=["POST", "OPTIONS"])
def salvar_grafico_termometro_gaps():
    if request.method == "OPTIONS":
        response = jsonify({'status': 'CORS preflight OK'})
        response.headers["Access-Control-Allow-Origin"] = "https://gestor.thehrkey.tech"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
        response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
        return response

    try:
        # Importações necessárias para esta rota (verifique se já existem no topo do app.py)
        # Removido 'matplotlib.cm' se não for usado
        import pandas as pd # Já global
        import numpy as np
        import matplotlib.pyplot as plt
        import requests
        from datetime import datetime, timedelta
        import base64 # Para imagem base64
        import os # Para os.remove
        from statistics import mean # Para cálculos de média

        dados_requisicao = request.get_json()
        empresa = dados_requisicao.get("empresa")
        codrodada = dados_requisicao.get("codrodada")
        emaillider_req = dados_requisicao.get("emailLider") # Usando emaillider_req para consistência

        if not all([empresa, codrodada, emaillider_req]):
            return jsonify({"erro": "Campos obrigatórios ausentes."}), 400

        # --- Lógica de Caching: Buscar JSON do Gráfico Salvo ---
        tipo_relatorio_grafico_atual = "microambiente_termometro_gaps" # Identificador único para este gráfico

        # SUPABASE_REST_URL e SUPABASE_KEY são variáveis globais
        url_busca_cache = f"{SUPABASE_REST_URL}/relatorios_gerados"

        headers_cache_busca = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }

        params_cache = {
            "empresa": f"eq.{empresa}",
            "codrodada": f"eq.{codrodada}",
            "emaillider": f"eq.{emaillider_req}", # Variável emaillider_req
            "tipo_relatorio": f"eq.{tipo_relatorio_grafico_atual}",
            "order": "data_criacao.desc",
            "limit": 1
        }

        print(f"DEBUG: Buscando cache do gráfico '{tipo_relatorio_grafico_atual}' no Supabase...")
        cache_response = requests.get(url_busca_cache, headers=headers_cache_busca, params=params_cache, timeout=15)
        cache_response.raise_for_status()
        cached_data_list = cache_response.json()

        if cached_data_list:
            cached_report = cached_data_list[0]
            data_criacao_cache_str = cached_report.get("data_criacao", "")
            
            if data_criacao_cache_str:
                data_criacao = datetime.fromisoformat(data_criacao_cache_str.replace('Z', '+00:00')) 
                cache_validity_period = timedelta(hours=1) # Cache válido por 1 hora

                if datetime.now(data_criacao.tzinfo) - data_criacao < cache_validity_period:
                    print(f"✅ Cache válido encontrado para o gráfico '{tipo_relatorio_grafico_atual}'. Retornando dados cacheados.")
                    return jsonify(cached_report.get("dados_json", {})), 200
                else:
                    print(f"Cache do gráfico '{tipo_relatorio_grafico_atual}' expirado. Recalculando...")
            else:
                print("Cache encontrado, mas sem data de criação válida. Recalculando...")
        else:
            print(f"Cache do gráfico '{tipo_relatorio_grafico_atual}' não encontrado. Recalculando...")

        # --- Buscar consolidado de microambiente ---
        url_consolidado = f"{SUPABASE_REST_URL}/consolidado_microambiente" # Usando GLOBAL SUPABASE_REST_URL
        
        params_cons = {
            "empresa": f"eq.{empresa}",
            "codrodada": f"eq.{codrodada}",
            "emaillider": f"eq.{emaillider_req}" # Usando emaillider_req
        }

        print(f"DEBUG: Buscando consolidado de microambiente no Supabase para Empresa: {empresa}, Rodada: {codrodada}, Líder: {emaillider_req}")
        
        headers_consolidado_busca = {
            "apikey": SUPABASE_KEY, # Usando GLOBAL SUPABASE_KEY
            "Authorization": f"Bearer {SUPABASE_KEY}" # Usando GLOBAL SUPABASE_KEY
        }

        resp_consolidado = requests.get(url_consolidado, headers=headers_consolidado_busca, params=params_cons, timeout=30)
        resp_consolidado.raise_for_status()
        dados_consolidado = resp_consolidado.json()

        if not dados_consolidado:
            return jsonify({"erro": "Consolidado não encontrado."}), 404

        microambiente_consolidado = dados_consolidado[-1].get("dados_json", {}) # Assumindo que o JSON consolidado está em 'dados_json'
        
        # Extrair respostas para autoavaliação e equipe do JSON consolidado ANINHADO
        avaliacoes = microambiente_consolidado.get("avaliacoesEquipe", [])
        
        if not avaliacoes:
            return jsonify({"erro": "Nenhuma avaliação de equipe encontrada no consolidado."}), 400

        # --- Usar DataFrames globais para as matrizes ---
        matriz = MATRIZ_MICROAMBIENTE_DF # Usando global
        pontos_dim = TABELA_DIMENSAO_MICROAMBIENTE_DF # Usando global
        pontos_subdim = TABELA_SUBDIMENSAO_MICROAMBIENTE_DF # Usando global para subdimensoes


        gap_count = 0
        num_avaliacoes = len(avaliacoes)

        for i in range(1, 49): # Iterar sobre as questões Q01 a Q48
            q = f"Q{i:02d}"
            q_real = f"{q}C"
            q_ideal = f"{q}k"

            # Converte as respostas para INT de forma segura (para equipe)
            reais = []
            for av in avaliacoes:
                val_str = av.get(q_real)
                if val_str is not None and isinstance(val_str, str) and val_str.strip().isdigit():
                    reais.append(int(val_str))
            
            ideais = []
            for av in avaliacoes:
                val_str = av.get(q_ideal)
                if val_str is not None and isinstance(val_str, str) and val_str.strip().isdigit():
                    ideais.append(int(val_str))

            if not reais or not ideais:
                continue # Pula a questão se não houver dados válidos

            media_real = round(sum(reais) / len(reais))
            media_ideal = round(sum(ideais) / len(ideais))
            
            chave = f"{q}_I{media_ideal}_R{media_real}"
            
            linha = matriz[matriz["CHAVE"] == chave]
            
            if not linha.empty:
                # O GAP na sua tabela é uma string, converta para float de forma segura
                # --- CORREÇÃO: Simplificar a conversão de gap_val ---
                gap_val = float(linha.iloc[0]["GAP"]) # Converte diretamente para float
                # --- FIM DA CORREÇÃO ---
                if abs(gap_val) > 20: # Usa o gap_val já numérico
                    gap_count += 1

        def classificar_microambiente(gaps):
            if gaps <= 3:
                return "ALTO ESTÍMULO"
            elif gaps <= 6:
                return "ESTÍMULO"
            elif gaps <= 9:
                return "NEUTRO"
            elif gaps <= 12:
                return "BAIXO ESTÍMULO"
            else:
                return "DESMOTIVAÇÃO"

        classificacao_texto = classificar_microambiente(gap_count)
        
        data_hora = datetime.now().strftime("%d/%m/%Y %H:%M")

        # --- GERAÇÃO DO GRÁFICO (MATPLOTLIB) EM MEMÓRIA E CONVERSÃO PARA BASE64 ---
        # Este é o código do gráfico que você me enviou no HTML
        # Ajustado para usar as variáveis e nomes consistentes
        
        # Ajuste para plotar o termômetro (você deve ter o código que plota o termômetro aqui)
        # Exemplo de placeholder para plotting (SUBSTITUA PELO SEU CÓDIGO REAL DE PLOTAGEM DO TERMÔMETRO)
        fig, ax = plt.subplots(figsize=(6, 6)) # Dimensões do termômetro
        ax.set_xlim(0, 10)
        ax.set_ylim(0, 10)
        ax.text(5, 5, f"GAPs: {gap_count}\nClassificação: {classificacao_texto}", 
                ha='center', va='center', fontsize=16, color='black')
        ax.axis('off') # Remove os eixos
        plt.title(f"TERMÔMETRO DE GAPS\n{empresa} - {codrodada} - {emaillider_req}", fontsize=14)
        plt.tight_layout()
        
        # Salvar o gráfico em um buffer de memória e converter para base64
        import io # Importar io para BytesIO
        from PIL import Image # Importar Pillow para lidar com imagem

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight') # Salva em PNG no buffer
        plt.close(fig) # Fecha a figura para liberar memória
        buf.seek(0)
        imagem_base64 = base64.b64encode(buf.read()).decode("utf-8")


        dados_json_retorno = { # Renomeado para evitar conflito com 'dados_json' usado no payload
            "titulo": "STATUS - TERMÔMETRO DE MICROAMBIENTE",
            "subtitulo": f"{empresa} / {emaillider_req} / {codrodada} / {data_hora}",
            "info_avaliacoes": f"Equipe: {num_avaliacoes} respondentes",
            "qtdGapsAcima20": gap_count,
            "porcentagemGaps": round((gap_count / 48) * 100, 1),
            "classificacao": classificacao_texto,
            "imagemBase64": f"data:image/png;base64,{imagem_base64}" # Prefixo para uso direto no HTML img src
        }
        
        # --- Chamar a função para salvar os dados do gráfico gerados no Supabase ---
        salvar_json_no_supabase(dados_json_retorno, empresa, codrodada, emaillider_req, tipo_relatorio_grafico_atual)

        # Retornando o JSON completo para o navegador
        return jsonify(dados_json_retorno), 200

    except Exception as e:
        detailed_traceback = traceback.format_exc()
        print("\n" + "="*50)
        print("🚨🚨🚨 ERRO CRÍTICO NA ROTA salvar-grafico-termometro-gaps 🚨🚨🚨")
        print(f"Tipo do erro: {type(e).__name__}")
        print(f"Mensagem do erro: {str(e)}")
        print("TRACEBACK COMPLETO ABAIXO:")
        traceback.print_exc()
        print("="*50 + "\n")
        
        return jsonify({"erro": str(e), "debug_info": "Verifique os logs do Render.com para detalhes."}), 500


@app.route("/salvar-consolidado-microambiente", methods=["POST"])
def salvar_consolidado_microambiente():
    try:
        import requests
        from datetime import datetime

        dados = request.get_json()
        empresa = dados.get("empresa", "").strip().lower()
        codrodada = dados.get("codrodada", "").strip().lower()
        emailLider = dados.get("emailLider", "").strip().lower()

        print(f"✅ Dados recebidos: {empresa} {codrodada} {emailLider}")
        print("🔁 Iniciando chamada ao Supabase com os dados validados...")

        supabase_url = os.environ.get("SUPABASE_REST_URL")
        supabase_key = os.environ.get("SUPABASE_KEY")

        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json"
        }

        # 🔍 Buscar autoavaliação
        filtro_auto = f"?select=dados_json&empresa=eq.{empresa}&codrodada=eq.{codrodada}&emailLider=eq.{emailLider}&tipo=ilike.microambiente_autoavaliacao"
        url_auto = f"{supabase_url}/relatorios_microambiente{filtro_auto}"
        resp_auto = requests.get(url_auto, headers=headers)
        auto_data = resp_auto.json()
        print("📥 Resultado da requisição AUTO:", auto_data)

        if not auto_data:
            print("❌ microambiente_autoavaliacao não encontrada.")
            return jsonify({"erro": "microambiente_autoavaliacao não encontrada."}), 404

        autoavaliacao = auto_data[0]["dados_json"]

        # 🔍 Buscar avaliações de equipe
        filtro_equipe = f"?select=dados_json&empresa=eq.{empresa}&codrodada=eq.{codrodada}&emailLider=eq.{emailLider}&tipo=eq.microambiente_equipe"
        url_equipe = f"{supabase_url}/relatorios_microambiente{filtro_equipe}"
        resp_equipe = requests.get(url_equipe, headers=headers)
        equipe_data = resp_equipe.json()
        print("📥 Resultado da requisição EQUIPE:", equipe_data)

        avaliacoes_equipe = [r["dados_json"] for r in equipe_data if "dados_json" in r]

        if not avaliacoes_equipe:
            print("❌ Nenhuma avaliação de equipe encontrada.")
            return jsonify({"erro": "Nenhuma avaliação de equipe encontrada."}), 404

        # 🧩 Montar JSON final
        consolidado = {
            "autoavaliacao": autoavaliacao,
            "avaliacoesEquipe": avaliacoes_equipe
        }
        
        # 💾 Salvar na tabela final
        payload = {
            "empresa": empresa,
            "codrodada": codrodada,
            "emaillider": emailLider,
            "dados_json": consolidado,
            "data_criacao": datetime.utcnow().isoformat(),
            "nome_arquivo": f"consolidado_{empresa}_{codrodada}_{emailLider}.json".lower()
        }

        url_final = f"{supabase_url}/consolidado_microambiente"
        resp_final = requests.post(url_final, headers=headers, json=payload)

        if resp_final.status_code not in [200, 201]:
            print("❌ Erro ao salvar no Supabase:", resp_final.text)
            return jsonify({"erro": "Erro ao salvar consolidado."}), 500

        print("✅ Consolidado salvo com sucesso.")
        return jsonify({"mensagem": "Consolidado salvo com sucesso."})

    except Exception as e:
        print("💥 ERRO GERAL:", str(e))
        return jsonify({"erro": str(e)}), 500

