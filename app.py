from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import io
import json
import os
import requests
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

app = Flask(__name__)
CORS(app, supports_credentials=True)

@app.after_request
def aplicar_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "https://gestor.thehrkey.tech"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    response.headers["Access-Control-Allow-Credentials"] = "true"
    return response

@app.route("/enviar-avaliacao", methods=["OPTIONS"])
def preflight():
    return '', 200

# Carregar planilhas
tabela_dim = pd.read_excel("pontos_maximos_dimensao.xlsx")
tabela_sub = pd.read_excel("pontos_maximos_subdimensao.xlsx")
matriz = pd.read_excel("TABELA_GERAL_MICROAMBIENTE_COM_CHAVE.xlsx")

@app.route("/")
def home():
    return "API Microambiente Online"

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

@app.route("/enviar-avaliacao", methods=["POST"])
def enviar_avaliacao():
    dados = request.get_json()
    if not dados:
        return jsonify({"erro": "Nenhum dado recebido"}), 400

    print("✅ Dados recebidos:", dados)

    url_script = "https://script.google.com/macros/s/AKfycbzrKBSwgRf9ckJrBDRkC1VsDibhYrWTJkLPhVMt83x_yCXnd_ex_CYuehT8pioTFvbxsw/exec"

    try:
        resposta = requests.post(url_script, json=dados)
        if resposta.status_code == 200:
            print("✅ Avaliação salva no Drive com sucesso!")
            return jsonify({"status": "✅ Microambiente de Equipes → salva no Drive"}), 200
        else:
            print("❌ Erro ao salvar no Drive:", resposta.text)
            return jsonify({"erro": "Erro ao salvar no Drive"}), 500
    except Exception as e:
        print("❌ Erro ao enviar dados:", str(e))
        return jsonify({"erro": str(e)}), 500

# 🔽 NOVA ROTA DE RELATÓRIO CONSOLIDADO DE MICROAMBIENTE
@app.route("/gerar-relatorio-microambiente", methods=["POST"])
def gerar_relatorio_microambiente():
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
        PASTA_RAIZ = "1l4kOZwed-Yc5nHU4RBTmWQz3zYAlpniS"

        def buscar_id(nome_pasta, id_pai):
            query = f"'{id_pai}' in parents and name = '{nome_pasta}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
            resp = service.files().list(q=query, fields="files(id)").execute().get("files", [])
            return resp[0]["id"] if resp else None

        id_empresa = buscar_id(empresa, PASTA_RAIZ)
        id_rodada = buscar_id(codrodada, id_empresa)
        id_lider = buscar_id(emailLider, id_rodada)

        if not id_lider:
            return jsonify({"erro": "Pasta do líder não encontrada"}), 404

        arquivos = service.files().list(
            q=f"'{id_lider}' in parents and (mimeType='application/json' or mimeType='text/plain') and trashed = false",
            fields="files(id, name)").execute().get("files", [])

        auto = None
        equipe = []

        for arq in arquivos:
            nome = arq["name"]
            arq_id = arq["id"]
            req = service.files().get_media(fileId=arq_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, req)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            fh.seek(0)
            conteudo = json.load(fh)
            tipo = conteudo.get("tipo", "").lower()
            if not tipo.startswith("microambiente"):
                continue  # ignora arquivos que não são de microambiente

            if "auto" in tipo:
                auto = conteudo
            elif "equipe" in tipo:
                equipe.append(conteudo)


        relatorio = {
            "empresa": empresa,
            "codrodada": codrodada,
            "emailLider": emailLider,
            "autoavaliacao": auto,
            "avaliacoesEquipe": equipe,
            "mensagem": "✅ Relatório consolidado microambiente gerado com sucesso"
        }

        nome_arquivo = f"relatorio_microambiente_{emailLider}_{codrodada}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        binario = json.dumps(relatorio, indent=2, ensure_ascii=False).encode("utf-8")
        media = MediaIoBaseUpload(io.BytesIO(binario), mimetype="application/json")
        metadata = {"name": nome_arquivo, "parents": [id_lider]}
        service.files().create(body=metadata, media_body=media).execute()

        return jsonify({"mensagem": "✅ Relatório consolidado salvo no Drive com sucesso."})

    except Exception as e:
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
@app.route("/salvar-grafico-autoavaliacao", methods=["POST"])
def salvar_grafico_autoavaliacao():
    try:
        from matplotlib import pyplot as plt
        import matplotlib.ticker as mticker
        import tempfile

        dados = request.get_json()
        empresa = dados.get("empresa")
        codrodada = dados.get("codrodada")
        emailLider = dados.get("emailLider")  # Corrigido aqui

        if not all([empresa, codrodada, emailLider]):
            return jsonify({"erro": "Campos obrigatórios ausentes."}), 400

        # --- GOOGLE DRIVE ---
        SCOPES = ['https://www.googleapis.com/auth/drive']
        creds = service_account.Credentials.from_service_account_info(
            json.loads(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")),
            scopes=SCOPES
        )
        service = build('drive', 'v3', credentials=creds)
        PASTA_RAIZ = "1l4kOZwed-Yc5nHU4RBTmWQz3zYAlpniS"

        def buscar_id(nome, pai):
            q = f"'{pai}' in parents and name='{nome}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            resp = service.files().list(q=q, fields="files(id)").execute().get("files", [])
            return resp[0]["id"] if resp else None

        id_empresa = buscar_id(empresa.lower(), PASTA_RAIZ)
        id_rodada = buscar_id(codrodada.lower(), id_empresa)
        id_lider = buscar_id(emailLider.lower(), id_rodada)  # Corrigido aqui

        if not id_lider:
            return jsonify({"erro": "Pasta do líder não encontrada."}), 404

        arquivos = service.files().list(
            q=f"'{id_lider}' in parents and mimeType='application/json' and trashed = false",
            fields="files(id, name)").execute().get("files", [])

        auto = None
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
                auto = conteudo.get("autoavaliacao")
                break


        if not auto:
            return jsonify({"erro": "Autoavaliação não encontrada."}), 404

        matriz = pd.read_excel("TABELA_GERAL_MICROAMBIENTE_COM_CHAVE.xlsx")
        pontos_dim = pd.read_excel("pontos_maximos_dimensao.xlsx")

        calculo = []
        for i in range(1, 49):
            q_campo = f"Q{i:02d}C"
            q_kampo = f"Q{i:02d}k"
            if q_campo in auto and q_kampo in auto:
                real = int(auto[q_campo])
                ideal = int(auto[q_kampo])
                chave = f"Q{i:02d}_I{ideal}_R{real}"
                linha = matriz[matriz["CHAVE"] == chave]
                if not linha.empty:
                    dim = linha.iloc[0]["DIMENSAO"]
                    pi = linha.iloc[0]["PONTUACAO_IDEAL"]
                    pr = linha.iloc[0]["PONTUACAO_REAL"]
                    calculo.append((dim, pi, pr))

        df = pd.DataFrame(calculo, columns=["DIMENSAO", "IDEAL", "REAL"])
        resultado = df.groupby("DIMENSAO").sum().reset_index()
        resultado = resultado.merge(pontos_dim, on="DIMENSAO")
        resultado["IDEAL_%"] = (resultado["IDEAL"] / resultado["PONTOS_MAXIMOS_DIMENSAO"] * 100).round(1)
        resultado["REAL_%"] = (resultado["REAL"] / resultado["PONTOS_MAXIMOS_DIMENSAO"] * 100).round(1)

        # --- GERAR GRÁFICO ---
        fig, ax = plt.subplots(figsize=(10, 6))
        x = resultado["DIMENSAO"]
        ax.plot(x, resultado["REAL_%"], label="Como é", color="navy", marker='o')
        ax.plot(x, resultado["IDEAL_%"], label="Como deveria ser", color="orange", marker='o')

        for i, v in enumerate(resultado["REAL_%"]):
            ax.text(i, v + 1.5, f"{v}%", ha='center', fontsize=8)
        for i, v in enumerate(resultado["IDEAL_%"]):
            ax.text(i, v + 1.5, f"{v}%", ha='center', fontsize=8)

        # Linha de referência no gráfico
        ax.axhline(60, color="gray", linestyle="--", linewidth=1)

        # Limites e marcações do eixo Y
        ax.set_ylim(0, 100)
        ax.yaxis.set_major_locator(mticker.MultipleLocator(10))

        # TÍTULO e SUBTÍTULO
        fig.suptitle("MICROAMBIENTE DE EQUIPES - DIMENSÕES", fontsize=14, weight="bold", y=0.97)
        # ax.set_title("Autoavaliação do líder - Percentual por dimensão", fontsize=11)

        # Ajuste de layout para não cortar os títulos
        plt.tight_layout(rect=[0, 0, 1, 0.93])




        numero_avaliacoes = 1

        

        data_hora = datetime.now().strftime("%d/%m/%Y %H:%M")
        ax.text(0.5, 1.05, f"Empresa: {empresa}", transform=ax.transAxes, ha="center", fontsize=10)
        ax.text(0.5, 1.01, f"Autoavaliação - Líder: {emailLider} - Rodada: {codrodada} - {data_hora} - |  N = {numero_avaliacoes}",
                transform=ax.transAxes, ha="center", fontsize=9)
        

        ax.set_facecolor("#f2f2f2")
        fig.patch.set_facecolor('#f2f2f2')
        ax.legend()
        plt.tight_layout()

        # SALVAR E ENVIAR PARA O DRIVE
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            caminho_pdf = tmp.name
            plt.savefig(caminho_pdf, format="pdf")

        nome_pdf = f"grafico_microambiente_autoavaliacao_{emailLider}_{codrodada}.pdf"
        with open(caminho_pdf, "rb") as f:
            media = MediaIoBaseUpload(f, mimetype="application/pdf")
            metadata = {"name": nome_pdf, "parents": [id_lider]}
            service.files().create(body=metadata, media_body=media).execute()

        os.remove(caminho_pdf)
        return jsonify({"mensagem": "✅ Gráfico gerado e salvo no Drive com sucesso."})

    except Exception as e:
        return jsonify({"erro": str(e)}), 500


@app.route("/salvar-grafico-autoavaliacao-subdimensao", methods=["POST"])
def salvar_grafico_autoavaliacao_subdimensao():
    try:
        from matplotlib import pyplot as plt
        import matplotlib.ticker as mticker
        import tempfile

        dados = request.get_json()
        empresa = dados.get("empresa")
        codrodada = dados.get("codrodada")
        emailLider = dados.get("emailLider")

        if not all([empresa, codrodada, emailLider]):
            return jsonify({"erro": "Campos obrigatórios ausentes."}), 400

        # --- GOOGLE DRIVE ---
        SCOPES = ['https://www.googleapis.com/auth/drive']
        creds = service_account.Credentials.from_service_account_info(
            json.loads(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")),
            scopes=SCOPES
        )
        service = build('drive', 'v3', credentials=creds)
        PASTA_RAIZ = "1l4kOZwed-Yc5nHU4RBTmWQz3zYAlpniS"

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

        auto = None
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
                auto = conteudo.get("autoavaliacao")
                break

        if not auto:
            return jsonify({"erro": "Autoavaliação não encontrada."}), 404

        matriz = pd.read_excel("TABELA_GERAL_MICROAMBIENTE_COM_CHAVE.xlsx")
        pontos_sub = pd.read_excel("pontos_maximos_subdimensao.xlsx")

        calculo = []
        for i in range(1, 49):
            q_campo = f"Q{i:02d}C"
            q_kampo = f"Q{i:02d}k"
            if q_campo in auto and q_kampo in auto:
                real = int(auto[q_campo])
                ideal = int(auto[q_kampo])
                chave = f"Q{i:02d}_I{ideal}_R{real}"
                linha = matriz[matriz["CHAVE"] == chave]
                if not linha.empty:
                    sub = linha.iloc[0]["SUBDIMENSAO"]
                    pi = linha.iloc[0]["PONTUACAO_IDEAL"]
                    pr = linha.iloc[0]["PONTUACAO_REAL"]
                    calculo.append((sub, pi, pr))

        df = pd.DataFrame(calculo, columns=["SUBDIMENSAO", "IDEAL", "REAL"])
        resultado = df.groupby("SUBDIMENSAO").sum().reset_index()
        resultado = resultado.merge(pontos_sub, on="SUBDIMENSAO")
        resultado["IDEAL_%"] = (resultado["IDEAL"] / resultado["PONTOS_MAXIMOS_SUBDIMENSAO"] * 100).round(1)
        resultado["REAL_%"] = (resultado["REAL"] / resultado["PONTOS_MAXIMOS_SUBDIMENSAO"] * 100).round(1)

        # --- GRÁFICO ---
        fig, ax = plt.subplots(figsize=(12, 6))
        x = resultado["SUBDIMENSAO"]
        ax.plot(x, resultado["REAL_%"], label="Como é", color="navy", marker='o')
        ax.plot(x, resultado["IDEAL_%"], label="Como deveria ser", color="orange", marker='o')

        ax.set_xticks(range(len(x)))
        ax.set_xticklabels(x, rotation=45, ha="right", fontsize=9)


        for i, v in enumerate(resultado["REAL_%"]):
            ax.text(i, v + 1.5, f"{v}%", ha='center', fontsize=8)
        for i, v in enumerate(resultado["IDEAL_%"]):
            ax.text(i, v + 1.5, f"{v}%", ha='center', fontsize=8)

        ax.axhline(60, color="gray", linestyle="--", linewidth=1)
        ax.set_ylim(0, 100)
        ax.yaxis.set_major_locator(mticker.MultipleLocator(10))

        fig.suptitle("MICROAMBIENTE DE EQUIPES - SUBDIMENSÕES", fontsize=14, weight="bold", y=0.97)
        plt.tight_layout(rect=[0, 0, 1, 0.93])

        numero_avaliacoes = 1
        from datetime import datetime
        data_hora = datetime.now().strftime("%d/%m/%Y %H:%M")
        ax.text(0.5, 1.05, f"Empresa: {empresa}", transform=ax.transAxes, ha="center", fontsize=10)
        ax.text(0.5, 1.01, f"Autoavaliação - Líder: {emailLider} - Rodada: {codrodada} - {data_hora}  |  N = {numero_avaliacoes}",
                transform=ax.transAxes, ha="center", fontsize=9)

        ax.set_facecolor("#f2f2f2")
        fig.patch.set_facecolor('#f2f2f2')
        ax.legend()
        plt.tight_layout()

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            caminho_pdf = tmp.name
            plt.savefig(caminho_pdf, format="pdf")

        nome_pdf = f"grafico_microambiente_autoavaliacao_subdimensao_{emailLider}_{codrodada}.pdf"
        with open(caminho_pdf, "rb") as f:
            media = MediaIoBaseUpload(f, mimetype="application/pdf")
            metadata = {"name": nome_pdf, "parents": [id_lider]}
            service.files().create(body=metadata, media_body=media).execute()

        os.remove(caminho_pdf)
        return jsonify({"mensagem": "✅ Gráfico de subdimensões gerado e salvo no Drive com sucesso."})

    except Exception as e:
        return jsonify({"erro": str(e)}), 500


@app.route("/salvar-grafico-media-equipe-dimensao", methods=["POST"])
def salvar_grafico_media_equipe_dimensao():
    try:
        from matplotlib import pyplot as plt
        import matplotlib.ticker as mticker
        import tempfile
        from statistics import mean

        dados = request.get_json()
        empresa = dados.get("empresa")
        codrodada = dados.get("codrodada")
        emailLider = dados.get("emailLider")

        if not all([empresa, codrodada, emailLider]):
            return jsonify({"erro": "Campos obrigatórios ausentes."}), 400

        # --- GOOGLE DRIVE ---
        SCOPES = ['https://www.googleapis.com/auth/drive']
        creds = service_account.Credentials.from_service_account_info(
            json.loads(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")),
            scopes=SCOPES
        )
        service = build('drive', 'v3', credentials=creds)
        PASTA_RAIZ = "1l4kOZwed-Yc5nHU4RBTmWQz3zYAlpniS"

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

        avaliacoes = []
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
                avaliacoes = conteudo.get("avaliacoesEquipe", [])
                break

        if not avaliacoes:
            return jsonify({"erro": "Avaliações da equipe não encontradas."}), 404

        matriz = pd.read_excel("TABELA_GERAL_MICROAMBIENTE_COM_CHAVE.xlsx")
        pontos_dim = pd.read_excel("pontos_maximos_dimensao.xlsx")

        calculo = []
        for i in range(1, 49):
            q = f"Q{i:02d}"
            q_real = f"{q}C"
            q_ideal = f"{q}k"

            valores_real = [int(av[q_real]) for av in avaliacoes if q_real in av]
            valores_ideal = [int(av[q_ideal]) for av in avaliacoes if q_ideal in av]

            if not valores_real or not valores_ideal:
                continue

            media_real = round(mean(valores_real))
            media_ideal = round(mean(valores_ideal))
            chave = f"{q}_I{media_ideal}_R{media_real}"

            linha = matriz[matriz["CHAVE"] == chave]
            if not linha.empty:
                dim = linha.iloc[0]["DIMENSAO"]
                pi = linha.iloc[0]["PONTUACAO_IDEAL"]
                pr = linha.iloc[0]["PONTUACAO_REAL"]
                calculo.append((dim, pi, pr))

        df = pd.DataFrame(calculo, columns=["DIMENSAO", "IDEAL", "REAL"])
        resultado = df.groupby("DIMENSAO").sum().reset_index()
        resultado = resultado.merge(pontos_dim, on="DIMENSAO")
        resultado["IDEAL_%"] = (resultado["IDEAL"] / resultado["PONTOS_MAXIMOS_DIMENSAO"] * 100).round(1)
        resultado["REAL_%"] = (resultado["REAL"] / resultado["PONTOS_MAXIMOS_DIMENSAO"] * 100).round(1)

        # --- GRÁFICO ---
        fig, ax = plt.subplots(figsize=(10, 6))
        x = resultado["DIMENSAO"]
        ax.plot(x, resultado["REAL_%"], label="Como é", color="navy", marker='o')
        ax.plot(x, resultado["IDEAL_%"], label="Como deveria ser", color="orange", marker='o')

        for i, v in enumerate(resultado["REAL_%"]):
            ax.text(i, v + 1.5, f"{v}%", ha='center', fontsize=8)
        for i, v in enumerate(resultado["IDEAL_%"]):
            ax.text(i, v + 1.5, f"{v}%", ha='center', fontsize=8)

        ax.axhline(60, color="gray", linestyle="--", linewidth=1)
        ax.set_ylim(0, 100)
        ax.yaxis.set_major_locator(mticker.MultipleLocator(10))

        fig.suptitle("MÉDIA DA EQUIPE - DIMENSÕES", fontsize=14, weight="bold", y=0.97)
        plt.tight_layout(rect=[0, 0, 1, 0.93])

        data_hora = datetime.now().strftime("%d/%m/%Y %H:%M")
        numero_avaliacoes = len(avaliacoes)
        ax.text(0.5, 1.05, f"Empresa: {empresa}", transform=ax.transAxes, ha="center", fontsize=10)
        ax.text(0.5, 1.01, f"Média da Equipe - Rodada: {codrodada} - {data_hora}   |  N = {numero_avaliacoes}",
                transform=ax.transAxes, ha="center", fontsize=9)

        ax.set_facecolor("#f2f2f2")
        fig.patch.set_facecolor('#f2f2f2')
        ax.legend()
        plt.tight_layout()

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            caminho_pdf = tmp.name
            plt.savefig(caminho_pdf, format="pdf")

        nome_pdf = f"grafico_microambiente_mediaequipe_dimensao_{emailLider}_{codrodada}.pdf"
        with open(caminho_pdf, "rb") as f:
            media = MediaIoBaseUpload(f, mimetype="application/pdf")
            metadata = {"name": nome_pdf, "parents": [id_lider]}
            service.files().create(body=metadata, media_body=media).execute()

        os.remove(caminho_pdf)
        return jsonify({"mensagem": "✅ Gráfico da Média da Equipe gerado com sucesso."})

    except Exception as e:
        return jsonify({"erro": str(e)}), 500


@app.route("/salvar-grafico-media-equipe-subdimensao", methods=["POST"])
def salvar_grafico_media_equipe_subdimensao():
    try:
        from matplotlib import pyplot as plt
        import matplotlib.ticker as mticker
        import tempfile

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
        PASTA_RAIZ = "1l4kOZwed-Yc5nHU4RBTmWQz3zYAlpniS"

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
            return jsonify({"erro": "Nenhuma avaliação de equipe encontrada."}), 404

        matriz = pd.read_excel("TABELA_GERAL_MICROAMBIENTE_COM_CHAVE.xlsx")
        pontos_max = pd.read_excel("pontos_maximos_subdimensao.xlsx")

        calculo = []
        for i in range(1, 49):
            q = f"Q{i:02d}"
            reais = []
            ideais = []
            for equipe in dados_equipes:
                if f"{q}C" in equipe and f"{q}k" in equipe:
                    reais.append(int(equipe[f"{q}C"]))
                    ideais.append(int(equipe[f"{q}k"]))

            if reais and ideais:
                media_real = round(sum(reais) / len(reais))
                media_ideal = round(sum(ideais) / len(ideais))
                chave = f"{q}_I{media_ideal}_R{media_real}"
                linha = matriz[matriz["CHAVE"] == chave]
                if not linha.empty:
                    sub = linha.iloc[0]["SUBDIMENSAO"]
                    pi = linha.iloc[0]["PONTUACAO_IDEAL"]
                    pr = linha.iloc[0]["PONTUACAO_REAL"]
                    calculo.append((sub, pi, pr))

        df = pd.DataFrame(calculo, columns=["SUBDIMENSAO", "IDEAL", "REAL"])
        resultado = df.groupby("SUBDIMENSAO").sum().reset_index()
        resultado = resultado.merge(pontos_max, on="SUBDIMENSAO")
        resultado["IDEAL_%"] = (resultado["IDEAL"] / resultado["PONTOS_MAXIMOS_SUBDIMENSAO"] * 100).round(1)
        resultado["REAL_%"] = (resultado["REAL"] / resultado["PONTOS_MAXIMOS_SUBDIMENSAO"] * 100).round(1)

        # GRÁFICO
        fig, ax = plt.subplots(figsize=(10, 6))
        x = resultado["SUBDIMENSAO"]
        ax.plot(x, resultado["REAL_%"], label="Como é", color="navy", marker='o')
        ax.plot(x, resultado["IDEAL_%"], label="Como deveria ser", color="orange", marker='o')

        for i, v in enumerate(resultado["REAL_%"]):
            ax.text(i, v + 1.5, f"{v}%", ha='center', fontsize=8)
        for i, v in enumerate(resultado["IDEAL_%"]):
            ax.text(i, v + 1.5, f"{v}%", ha='center', fontsize=8)

        ax.axhline(60, color="gray", linestyle="--", linewidth=1)
        ax.set_ylim(0, 100)
        ax.yaxis.set_major_locator(mticker.MultipleLocator(10))

        fig.suptitle("MICROAMBIENTE DE EQUIPES - SUBDIMENSÕES", fontsize=14, weight="bold", y=0.97)

        plt.xticks(rotation=45)
        plt.tight_layout(rect=[0, 0, 1, 0.93])

        data_hora = datetime.now().strftime("%d/%m/%Y %H:%M")
        numero_avaliacoes = len(dados_equipes)

        ax.text(0.5, 1.05, f"Empresa: {empresa}", transform=ax.transAxes, ha="center", fontsize=10)
        ax.text(0.5, 1.01, f"Média da Equipe - Líder: {emailLider} - Rodada: {codrodada} - {data_hora} | N = {numero_avaliacoes}",
                transform=ax.transAxes, ha="center", fontsize=9)

        ax.set_facecolor("#f2f2f2")
        fig.patch.set_facecolor('#f2f2f2')
        ax.legend()

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            caminho_pdf = tmp.name
            plt.savefig(caminho_pdf, format="pdf")

        nome_pdf = f"grafico_microambiente_media_equipe_subdimensao_{emailLider}_{codrodada}.pdf"
        with open(caminho_pdf, "rb") as f:
            media = MediaIoBaseUpload(f, mimetype="application/pdf")
            metadata = {"name": nome_pdf, "parents": [id_lider]}
            service.files().create(body=metadata, media_body=media).execute()

        os.remove(caminho_pdf)
        return jsonify({"mensagem": "✅ Gráfico de subdimensões gerado com sucesso!"})

    except Exception as e:
        return jsonify({"erro": str(e)}), 500


@app.route("/grafico-waterfall-gaps", methods=["POST"])
def grafico_waterfall_gaps():
    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    import seaborn as sns
    import json, io, os
    from flask import request, jsonify
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
    from google.oauth2 import service_account

    try:
        # --- REQUISIÇÃO E VALIDAÇÃO ---
        dados = request.get_json()
        empresa = dados.get("empresa")
        codrodada = dados.get("codrodada")
        emailLider = dados.get("emailLider")

        if not all([empresa, codrodada, emailLider]):
            return jsonify({"erro": "Campos obrigatórios ausentes."}), 400

        # --- CREDENCIAIS E DRIVE ---
        SCOPES = ['https://www.googleapis.com/auth/drive']
        creds = service_account.Credentials.from_service_account_info(
            json.loads(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")),
            scopes=SCOPES
        )
        service = build('drive', 'v3', credentials=creds)
        PASTA_RAIZ = "1l4kOZwed-Yc5nHU4RBTmWQz3zYAlpniS"

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

        # --- LOCALIZAR JSON VÁLIDO ---
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
            return jsonify({"erro": "Nenhuma avaliação de equipe encontrada."}), 400

        num_avaliacoes = len(dados_equipes)

        # --- CARREGAR MATRIZ ---
        matriz = pd.read_excel("TABELA_GERAL_MICROAMBIENTE_COM_CHAVE.xlsx")

        # --- CÁLCULO DE MÉDIAS POR QUESTÃO ---
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
                    "DIMENSAO": row["DIMENSAO"],
                    "SUBDIMENSAO": row["SUBDIMENSAO"],
                    "GAP": row["GAP"]
                })

        base = pd.DataFrame(registros)
        gap_dim = base.groupby("DIMENSAO")["GAP"].mean().reset_index().sort_values("GAP")
        gap_subdim = base.groupby("SUBDIMENSAO")["GAP"].mean().reset_index().sort_values("GAP")

        # --- PLOTAGEM ---
        import matplotlib.pyplot as plt
        import seaborn as sns
        import matplotlib.ticker as mticker

        fig, (ax1, ax2) = plt.subplots(nrows=2, figsize=(14, 10))

        sns.barplot(x="DIMENSAO", y="GAP", data=gap_dim, palette="coolwarm", ax=ax1)
        ax1.set_title("Waterfall - GAP por Dimensão (Média da Equipe)", fontsize=14)
        ax1.set_ylabel("GAP Médio (%)")
        ax1.set_ylim(-100, 0)
        ax1.yaxis.set_major_locator(mticker.MultipleLocator(10))
        ax1.tick_params(axis='x', rotation=45)
        for bar in ax1.patches:
            height = bar.get_height()
            ax1.annotate(f'{height:.1f}%', (bar.get_x() + bar.get_width() / 2, height - 4),
                         ha='center', fontsize=8)

        sns.barplot(x="SUBDIMENSAO", y="GAP", data=gap_subdim, palette="viridis", ax=ax2)
        ax2.set_title("Waterfall - GAP por Subdimensão (Média da Equipe)", fontsize=14)
        ax2.set_ylabel("GAP Médio (%)")
        ax2.set_ylim(-100, 0)
        ax2.yaxis.set_major_locator(mticker.MultipleLocator(10))
        ax2.tick_params(axis='x', rotation=90)
        for bar in ax2.patches:
            height = bar.get_height()
            ax2.annotate(f'{height:.1f}%', (bar.get_x() + bar.get_width() / 2, height - 4),
                         ha='center', fontsize=7)

        plt.tight_layout()
        nome_arquivo = f"waterfall_gaps_{emailLider}_{codrodada}.pdf"
        caminho_local = f"/tmp/{nome_arquivo}"

        # Inserir rodapé com informações do relatório
        fig.text(0.11, 0.01, 
                 f"{empresa} - {emailLider} - {codrodada} - {pd.Timestamp.now().strftime('%d/%m/%Y')}", 
                 ha='center', va='bottom', fontsize=8, color='gray', style='italic')
        plt.savefig(caminho_local)

        


        # --- UPLOAD PARA O DRIVE ---
        media = MediaIoBaseUpload(io.BytesIO(open(caminho_local, "rb").read()), mimetype="application/pdf")
        service.files().create(
            body={"name": nome_arquivo, "parents": [id_lider]},
            media_body=media,
            fields="id"
        ).execute()

        return jsonify({"mensagem": f"✅ Gráfico salvo no Drive: {nome_arquivo}"}), 200

    except Exception as e:
        return jsonify({"erro": str(e)}), 500



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
        PASTA_RAIZ = "1l4kOZwed-Yc5nHU4RBTmWQz3zYAlpniS"

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

        return jsonify({"mensagem": f"✅ Relatório salvo com sucesso no Google Drive: {nome_arquivo}"}), 200

    except Exception as e:
        return jsonify({"erro": str(e)}), 500


# Código Python completo para gerar o relatório analítico conforme layout aprovado

from flask import request, jsonify
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import json
import io
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from matplotlib.backends.backend_pdf import PdfPages

@app.route("/relatorio-analitico-microambiente", methods=["POST", "OPTIONS"])
def relatorio_analitico_microambiente():
    from flask import request, jsonify
    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    import seaborn as sns
    import json
    import io
    import os
    from datetime import datetime
    from matplotlib.backends.backend_pdf import PdfPages
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
        PASTA_RAIZ = "1l4kOZwed-Yc5nHU4RBTmWQz3zYAlpniS"

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
        df.sort_values(by=["DIMENSAO", "SUBDIMENSAO", "GAP"], inplace=True)

        nome_arquivo = f"relatorio_analitico_microambiente_{emailLider}_{codrodada}.pdf"
        caminho_local = f"/tmp/{nome_arquivo}"

        with PdfPages(caminho_local) as pdf:
            fig_capa, ax_capa = plt.subplots(figsize=(10, 6))
            ax_capa.axis("off")
            ax_capa.text(0.5, 0.65, "MICROAMBIENTE DE EQUIPES\nANÁLISE DE IMPACTOS", ha="center", va="center", fontsize=20, weight="bold")
            ax_capa.text(0.5, 0.4, f"{empresa} - {emailLider} - {codrodada} - {datetime.now().strftime('%d/%m/%Y')}",
                         ha="center", fontsize=10, style='italic')
            pdf.savefig(fig_capa)
            plt.close()

            for (dim, subdim), grupo in df.groupby(["DIMENSAO", "SUBDIMENSAO"]):
                fig, axs = plt.subplots(2, 2, figsize=(10, 8))
                axs = axs.flatten()
                fig.suptitle(f"AFIRMAÇÕES QUE IMPACTAM A SUBDIMENSÃO {subdim.upper()}", fontsize=12, weight="bold")

                for idx, (_, linha) in enumerate(grupo.iterrows()):
                    if idx >= 4:
                        break
                    ax = axs[idx]
                    ideal = linha["PONTUACAO_IDEAL"]
                    real = linha["PONTUACAO_REAL"]
                    gap = linha["GAP"]
                    titulo = f"{linha['QUESTAO']} - {linha['AFIRMACAO'][:60]}{'...' if len(linha['AFIRMACAO']) > 60 else ''}"

                    ax.barh(["Ideal"], [ideal], color="orange")
                    ax.barh(["Real"], [real], color="navy")
                    ax.barh(["GAP"], [abs(gap)], color="red" if abs(gap) > 20 else "green")

                    ax.set_xlim(0, 100)
                    ax.xaxis.set_major_locator(mticker.MultipleLocator(10))
                    ax.set_title(titulo, fontsize=8)
                    for bar in ax.containers:
                        for rect in bar:
                            width = rect.get_width()
                            ax.annotate(f'{width:.0f}%', xy=(width, rect.get_y() + rect.get_height() / 2),
                                        xytext=(3, 0), textcoords="offset points",
                                        ha='left', va='center', fontsize=6, color='black')

                plt.tight_layout(rect=[0, 0, 1, 0.95])
                pdf.savefig(fig)
                plt.close()

        file_metadata = {"name": nome_arquivo, "parents": [id_lider]}
        media = MediaIoBaseUpload(open(caminho_local, "rb"), mimetype="application/pdf")
        service.files().create(body=file_metadata, media_body=media, fields="id").execute()

        return jsonify({"mensagem": f"✅ Relatório salvo com sucesso no Google Drive: {nome_arquivo}"}), 200

    except Exception as e:
        return jsonify({"erro": str(e)}), 500


