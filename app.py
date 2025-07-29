import os
import json
import requests
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS # Não precisa de cross_origin aqui se usar CORS(app) globalmente
from datetime import datetime, timedelta
import traceback
from statistics import mean # Mantenha esta se você a utiliza em cálculos


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
        return response, 200

    try:
        # ... sua lógica principal (já está pronta)

        response = jsonify(dados_json)
        response.headers["Access-Control-Allow-Origin"] = "https://gestor.thehrkey.tech"
        return response, 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"erro": str(e)}), 500

        # Tipo de relatório
        tipo_relatorio_grafico_atual = "microambiente_waterfall_gaps"

        # Verifica cache no Supabase
        url_busca_cache = f"{SUPABASE_REST_URL}/relatorios_gerados"
        headers_cache = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }
        params_cache = {
            "empresa": f"eq.{empresa}",
            "codrodada": f"eq.{codrodada}",
            "emaillider": f"eq.{emailLider}",
            "tipo_relatorio": f"eq.{tipo_relatorio_grafico_atual}",
            "order": "data_criacao.desc",
            "limit": 1
        }

        import requests
        print(f"DEBUG: Verificando cache existente no Supabase...")
        cache_response = requests.get(url_busca_cache, headers=headers_cache, params=params_cache, timeout=15)
        cache_response.raise_for_status()
        cached_data = cache_response.json()

        if cached_data:
            data_criacao = cached_data[0].get("data_criacao")
            if data_criacao:
                from datetime import datetime
                data_cache = datetime.fromisoformat(data_criacao.replace('Z', '+00:00'))
                if datetime.now(data_cache.tzinfo) - data_cache < timedelta(hours=1):
                    print("✅ Cache válido encontrado. Retornando JSON.")
                    return jsonify(cached_data[0].get("dados_json", {})), 200

        # Buscar dados no consolidado_microambiente
        url_consolidado = f"{SUPABASE_REST_URL}/consolidado_microambiente"
        params_cons = {
            "empresa": f"eq.{empresa}",
            "codrodada": f"eq.{codrodada}",
            "emaillider": f"eq.{emailLider}"
        }

        print("DEBUG: Buscando consolidado_microambiente no Supabase...")
        cons_response = requests.get(url_consolidado, headers=headers_cache, params=params_cons, timeout=30)
        cons_response.raise_for_status()
        cons_data = cons_response.json()
        if not cons_data:
            return jsonify({"erro": "Consolidado não encontrado."}), 404

        dados_consolidado = cons_data[-1].get("dados_json", {})
        avaliacoes = dados_consolidado.get("avaliacoesEquipe", [])
        if not avaliacoes:
            return jsonify({"erro": "Nenhuma avaliação de equipe encontrada."}), 400

        matriz = MATRIZ_MICROAMBIENTE_DF  # Já carregada globalmente
        registros = []
        for i in range(1, 49):
            q = f"Q{i:02d}"
            q_ideal = f"{q}k"
            q_real = f"{q}C"

            valores_ideal = []
            valores_real = []
            for av in avaliacoes:
                vi = av.get(q_ideal)
                vr = av.get(q_real)
                if isinstance(vi, str) and vi.strip().isdigit():
                    valores_ideal.append(int(vi))
                if isinstance(vr, str) and vr.strip().isdigit():
                    valores_real.append(int(vr))

            mi = round(mean(valores_ideal)) if valores_ideal else 0
            mr = round(mean(valores_real)) if valores_real else 0
            chave = f"{q}_I{mi}_R{mr}"

            linha = matriz[matriz["CHAVE"] == chave]
            if not linha.empty:
                row = linha.iloc[0]
                registros.append({
                    "QUESTAO": q,
                    "DIMENSAO": row["DIMENSAO"],
                    "SUBDIMENSAO": row["SUBDIMENSAO"],
                    "GAP": float(row["GAP"])
                })

        base = pd.DataFrame(registros)
        gap_dim = base.groupby("DIMENSAO")["GAP"].mean().reset_index().sort_values("GAP")
        gap_subdim = base.groupby("SUBDIMENSAO")["GAP"].mean().reset_index().sort_values("GAP")

        data_hora = datetime.now().strftime("%d/%m/%Y %H:%M")

        
        print("DEBUG: gap_dim calculado:", gap_dim)
        print("DEBUG: gap_subdim calculado:", gap_subdim)

        
        # --- CONSTRUIR JSON DE RETORNO ---
        dados_json = {
            "titulo": "GAP MÉDIO POR DIMENSÃO E SUBDIMENSÃO",
            "subtitulo": f"{empresa} / {emailLider} / {codrodada} / {pd.Timestamp.now().strftime('%d/%m/%Y')}",
            "dados": {
                "dimensao": gap_dim.to_dict(orient="records"),
                "subdimensao": gap_subdim.to_dict(orient="records")
            }
        }

        salvar_json_no_supabase(dados_json, empresa, codrodada, emailLider, "microambiente_waterfall_gaps")

        # ✅ RESPOSTA COM CORS OK
        try:
    # ... (todo o seu código que monta o dados_json)
    
    # só aqui você cria e retorna
    response = jsonify(dados_json)
    response.headers["Access-Control-Allow-Origin"] = "https://gestor.thehrkey.tech"
    return response, 200
except Exception as e:
    import traceback
    traceback.print_exc()
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

@app.route("/relatorio-analitico-microambiente", methods=["POST", "OPTIONS"])
def relatorio_analitico_microambiente():
    from flask import request, jsonify
    import pandas as pd
    import matplotlib.pyplot as plt
    import json, io, os, tempfile
    from datetime import datetime
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import cm
    from reportlab.pdfgen import canvas
    from reportlab.platypus import Paragraph, Frame
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.enums import TA_JUSTIFY
    from reportlab.lib.colors import red
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

    if request.method == "OPTIONS":
        return '', 204

    try:
        dados = request.get_json()
        empresa = dados.get("empresa")
        codrodada = dados.get("codrodada")
        emailLider = dados.get("emailLider")

        if not all([empresa, codrodada, emailLider]):
            return jsonify({"erro": "Campos obrigatórios ausentes."}), 400

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
        df = df.sort_values(by=["DIMENSAO", "SUBDIMENSAO", "QUESTAO"])

        nome_pdf = f"relatorio_analitico_microambiente_{emailLider}_{codrodada}.pdf"
        caminho_local = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf").name
        c = canvas.Canvas(caminho_local, pagesize=A4)
        width, height = A4

        # --- CAPA ELEGANTE ---
        c.setFont("Helvetica-Bold", 16)
        c.drawCentredString(width / 2, height - 3.5 * cm, "RELATÓRIO CONSOLIDADO")
        c.drawCentredString(width / 2, height - 4.4 * cm, "DE MICROAMBIENTE")
        c.setFont("Helvetica", 10)
        subtitulo = f"{empresa} - {emailLider} - {codrodada} - {datetime.now().strftime('%d/%m/%Y')}"
        c.drawCentredString(width / 2, height - 6 * cm, subtitulo)
        c.showPage()

        styles = getSampleStyleSheet()
        estilo_questao = styles["Normal"]
        estilo_questao.fontName = "Helvetica"
        estilo_questao.fontSize = 11
        estilo_questao.leading = 14
        estilo_questao.alignment = TA_JUSTIFY

        grupo = df.groupby(["DIMENSAO", "SUBDIMENSAO"])

        for (dim, sub), bloco in grupo:
            c.setFont("Helvetica-Bold", 12)
            titulo = f"Questões que impactam a dimensão {dim} e subdimensão {sub}"
            c.drawCentredString(width / 2, height - 2 * cm, titulo)
            y = height - 4.0 * cm
            count = 0

            for _, linha in bloco.iterrows():
                if count == 6:
                    c.showPage()
                    c.setFont("Helvetica-Bold", 12)
                    c.drawCentredString(width / 2, height - 2 * cm, titulo)
                    y = height - 3.5 * cm
                    count = 0

                texto_afirmacao = f"{linha['QUESTAO']}: {linha['AFIRMACAO']}"
                frame = Frame(2 * cm, y - 1.2 * cm, width - 4 * cm, 2 * cm, showBoundary=0)
                paragrafo = Paragraph(texto_afirmacao, estilo_questao)
                frame.addFromList([paragrafo], c)
                y -= 1.0 * cm

                c.setFont("Helvetica", 9)
                texto = f"Como é: {linha['PONTUACAO_REAL']:.1f}%  |  Como deveria ser: {linha['PONTUACAO_IDEAL']:.1f}%  |  GAP: {linha['GAP']:.1f}%"
                c.drawString(2.5 * cm, y, texto)

                if abs(linha['GAP']) > 20:
                    c.setFillColor(red)
                    c.circle(width - 2 * cm, y + 0.2 * cm, 0.15 * cm, fill=1)
                    c.setFillColorRGB(0, 0, 0)

                y -= 3.0 * cm
                count += 1

            c.showPage()

        c.save()

        file_metadata = {"name": nome_pdf, "parents": [id_lider]}
        media = MediaIoBaseUpload(open(caminho_local, "rb"), mimetype="application/pdf")
        service.files().create(body=file_metadata, media_body=media, fields="id").execute()

                # Salvar também o JSON com prefixo IA_ na subpasta ia_json
        id_pasta_ia = buscar_id("ia_json", id_lider)
        if not id_pasta_ia:
            pasta = service.files().create(
                body={"name": "ia_json", "mimeType": "application/vnd.google-apps.folder", "parents": [id_lider]},
                fields="id"
            ).execute()
            id_pasta_ia = pasta["id"]

        dados_json = {
            "titulo": "RELATÓRIO ANALÍTICO DE MICROAMBIENTE",
            "subtitulo": f"{empresa} / {emailLider} / {codrodada} / {datetime.now().strftime('%d/%m/%Y')}",
            "numeroAvaliacoes": num_avaliacoes,
            "dados": registros
        }

        conteudo_json = json.dumps(dados_json, ensure_ascii=False, indent=2).encode("utf-8")
        nome_json = f"IA_relatorio_analitico_microambiente_{emailLider}_{codrodada}.json"
        media_json = MediaIoBaseUpload(io.BytesIO(conteudo_json), mimetype="application/json")
        service.files().create(
            body={"name": nome_json, "parents": [id_pasta_ia]},
            media_body=media_json,
            fields="id"
        ).execute()

        return jsonify({"mensagem": f"✅ Relatório salvo com sucesso no Google Drive: {nome_pdf}"}), 200


    except Exception as e:
        return jsonify({"erro": str(e)}), 500



@app.route("/termometro-microambiente", methods=["POST", "OPTIONS"])
def termometro_microambiente():
    from flask import request, jsonify

    if request.method == "OPTIONS":
        return '', 204

    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches
    import matplotlib.cm as cm
    import matplotlib.colors as mcolors
    import numpy as np
    import json, io, os, tempfile
    from datetime import datetime
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

    try:
        dados = request.get_json()
        empresa = dados.get("empresa")
        codrodada = dados.get("codrodada")
        emailLider = dados.get("emailLider")

        if not all([empresa, codrodada, emailLider]):
            return jsonify({"erro": "Campos obrigatórios ausentes."}), 400

        # Autenticação com o Google Drive
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
        gap_count = 0

        for i in range(1, 49):
            q = f"Q{i:02d}"
            media_ideal = round(somas[q]["ideal"] / num_avaliacoes)
            media_real = round(somas[q]["real"] / num_avaliacoes)
            chave = f"{q}_I{media_ideal}_R{media_real}"
            linha = matriz[matriz["CHAVE"] == chave]
            if not linha.empty:
                gap = float(linha.iloc[0]["GAP"])
                if abs(gap) > 20:
                    gap_count += 1

        def classificar_microambiente(gaps):
            if gaps <= 3:
                return "ALTO ESTÍMULO", "green"
            elif gaps <= 6:
                return "ESTÍMULO", "limegreen"
            elif gaps <= 9:
                return "NEUTRO", "orange"
            elif gaps <= 12:
                return "BAIXO ESTÍMULO", "orangered"
            else:
                return "DESMOTIVAÇÃO", "red"

        classificacao_texto, cor_texto = classificar_microambiente(gap_count)

        # Velocímetro semicircular
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.axis("off")

        total_gaps = 48
        angulo = np.linspace(0, np.pi, 500)
        raio = 1
        x = raio * np.cos(angulo)
        y = raio * np.sin(angulo)
        ax.plot(x, y, color='black', linewidth=2)

        cmap = cm.get_cmap('RdYlBu')

        cores = [cmap(i / total_gaps) for i in range(total_gaps + 1)]

        for i in range(total_gaps):
            start_ang = np.pi * i / total_gaps
            end_ang = np.pi * (i + 1) / total_gaps
            x_arc = [0] + list(raio * np.cos(np.linspace(start_ang, end_ang, 10)))
            y_arc = [0] + list(raio * np.sin(np.linspace(start_ang, end_ang, 10)))
            ax.fill(x_arc, y_arc, color=cores[i], edgecolor='none')

        ponteiro_ang = np.pi * (1 - gap_count / total_gaps)

        ax.plot([0, raio * np.cos(ponteiro_ang)], [0, raio * np.sin(ponteiro_ang)], color='black', linewidth=2)

        # Palavras dentro do velocímetro (posições específicas)
        faixas = [
            (4, "ALTO ESTÍMULO"),
            (8, "ESTÍMULO"),
            (13, "NEUTRO"),
            (17, "BAIXO ESTÍMULO"),
            (28, "DESMOTIVAÇÃO➜")
        ]

        for val, label in faixas:
            ang = np.pi * (1 - val / total_gaps)
            x = 0.7 * raio * np.cos(ang)
            y = 0.7 * raio * np.sin(ang)

            # ⚠️ Se for o label "DESMOTIVAÇÃO ➜", ajusta a posição para a esquerda
            if "DESMOTIVAÇÃO" in label:
                x -= 0.4  # desloca levemente à esquerda

            ax.text(x, y, label, fontsize=9, ha='center', va='center', weight='bold')

        



        # Marcação da escala de 0 a 48 ao longo do arco
        for val in range(0, total_gaps + 1):  # total_gaps = 48
            ang = np.pi * (1 - val / total_gaps)  # esquerda (0) → direita (48)
            ax.text(
                1.05 * raio * np.cos(ang),
                1.05 * raio * np.sin(ang),
                str(val),
                fontsize=6,
                ha='center',
                va='center'
            )


        ax.text(0, -0.2, f"{gap_count} GAPs ({(gap_count/48)*100:.1f}%)", ha='center', fontsize=12, weight='bold')
        ax.text(0, -0.35, f"Microambiente: {classificacao_texto}", ha='center', fontsize=11, color=cor_texto, weight='bold')

        nome_pdf = f"termometro_microambiente_{emailLider}_{codrodada}.pdf"
        caminho_pdf = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf").name
        fig.suptitle("STATUS - MICROAMBIENTE DE EQUIPE - QTD GAPS ACIMA DE 20%", fontsize=13, weight="bold")
        fig.text(0.2, 0.1, f"{empresa} - {emailLider} - {codrodada} - {datetime.now().strftime('%d/%m/%Y')}", fontsize=8, color="gray")
        plt.savefig(caminho_pdf, bbox_inches='tight')
        plt.close()

        file_metadata = {"name": nome_pdf, "parents": [id_lider]}
        media = MediaIoBaseUpload(open(caminho_pdf, "rb"), mimetype="application/pdf")
        service.files().create(body=file_metadata, media_body=media, fields="id").execute()

                # Salvar também o JSON com prefixo IA_ na subpasta ia_json
        id_pasta_ia = buscar_id("ia_json", id_lider)
        if not id_pasta_ia:
            pasta = service.files().create(
                body={"name": "ia_json", "mimeType": "application/vnd.google-apps.folder", "parents": [id_lider]},
                fields="id"
            ).execute()
            id_pasta_ia = pasta["id"]

        dados_json = {
            "titulo": "STATUS - TERMÔMETRO DE MICROAMBIENTE",
            "subtitulo": f"{empresa} / {emailLider} / {codrodada} / {datetime.now().strftime('%d/%m/%Y')}",
            "numeroAvaliacoes": num_avaliacoes,
            "qtdGapsAcima20": gap_count,
            "porcentagemGaps": round((gap_count / 48) * 100, 1),
            "classificacao": classificacao_texto
        }

        conteudo_json = json.dumps(dados_json, ensure_ascii=False, indent=2).encode("utf-8")
        nome_json = f"IA_termometro_microambiente_{emailLider}_{codrodada}.json"
        media_json = MediaIoBaseUpload(io.BytesIO(conteudo_json), mimetype="application/json")
        service.files().create(
            body={"name": nome_json, "parents": [id_pasta_ia]},
            media_body=media_json,
            fields="id"
        ).execute()

        return jsonify({"mensagem": f"✅ Termômetro salvo no Google Drive: {nome_pdf}"}), 200


    except Exception as e:
        return jsonify({"erro": str(e)}), 500


def salvar_json_ia_no_drive(dados, nome_pdf, service, id_lider):
    try:
        from io import BytesIO
        import json
        from googleapiclient.http import MediaIoBaseUpload

        # Criar subpasta "ia_json" se não existir
        query = f"'{id_lider}' in parents and name = 'ia_json' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        resposta = service.files().list(q=query, fields="files(id)").execute().get("files", [])
        if resposta:
            id_subpasta = resposta[0]["id"]
        else:
            pasta_metadata = {
                "name": "ia_json",
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [id_lider]
            }
            id_subpasta = service.files().create(body=pasta_metadata, fields="id").execute()["id"]

        # Prefixar com "IA_" e trocar extensão
        nome_json = "IA_" + nome_pdf.replace(".pdf", ".json")

        # Converter os dados em bytes
        conteudo = BytesIO(json.dumps(dados, indent=2, ensure_ascii=False).encode("utf-8"))
        media = MediaIoBaseUpload(conteudo, mimetype="application/json")

        # Enviar para subpasta "ia_json"
        file_metadata = {"name": nome_json, "parents": [id_subpasta]}
        service.files().create(body=file_metadata, media_body=media, fields="id").execute()

        print(f"✅ JSON IA salvo com sucesso: {nome_json}")
    except Exception as e:
        print("❌ Erro ao salvar JSON IA:", str(e))



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

