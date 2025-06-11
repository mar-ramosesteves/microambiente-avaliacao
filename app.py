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
        email_lider = dados.get("emailLider")

        if not all([empresa, codrodada, email_lider]):
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
        id_lider = buscar_id(email_lider, id_rodada)

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
            "emailLider": email_lider,
            "autoavaliacao": auto,
            "avaliacoesEquipe": equipe,
            "mensagem": "✅ Relatório consolidado microambiente gerado com sucesso"
        }

        nome_arquivo = f"relatorio_microambiente_{email_lider}_{codrodada}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
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
        pontos_maximos = pd.read_excel("pontos_maximos_dimensao_microambiente.xlsx")

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
        email_lider = auto.get("emailLider", "email")
        codrodada = auto.get("codrodada", "")
        data_hora = datetime.now().strftime("%d/%m/%Y %H:%M")
        subtitulo = f"Empresa: {empresa}   |   Autoavaliação - Líder: {email_lider} - Rodada: {codrodada} - {data_hora}   |   N = 1"
        plt.text(0.5, -0.18, subtitulo, ha="center", va="top", transform=ax.transAxes, fontsize=10)

        ax.legend()
        plt.xticks(rotation=20)
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
            nome = arq["name"].lower()
            if "microambiente" in nome.lower() and "auto" in nome.lower():

                req = service.files().get_media(fileId=arq["id"])
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, req)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
                fh.seek(0)
                auto = json.load(fh)
                break

        if not auto:
            return jsonify({"erro": "Autoavaliação não encontrada."}), 404

        matriz = pd.read_excel("TABELA_GERAL_MICROAMBIENTE_COM_CHAVE.xlsx")
        pontos_dim = pd.read_excel("pontos_maximos_dimensao_microambiente.xlsx")

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

        ax.axhline(60, color="gray", linestyle="--", linewidth=1)
        ax.set_ylim(0, 100)
        ax.yaxis.set_major_locator(mticker.MultipleLocator(10))
        ax.set_title("MICROAMBIENTE DE EQUIPES - DIMENSÕES", fontsize=14, weight="bold")

        data_hora = datetime.now().strftime("%d/%m/%Y %H:%M")
        ax.text(0.5, 1.05, f"Empresa: {empresa}", transform=ax.transAxes, ha="center", fontsize=10)
        ax.text(0.5, 1.01, f"Autoavaliação - Líder: {emailLider} - Rodada: {codrodada} - {data_hora}",
                transform=ax.transAxes, ha="center", fontsize=9)
        ax.text(0.5, 0.97, "N = 1", transform=ax.transAxes, ha="center", fontsize=9)

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
