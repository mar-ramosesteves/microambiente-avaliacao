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
from io import BytesIO

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ["https://gestor.thehrkey.tech"]}}, supports_credentials=True)

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
        PASTA_RAIZ = "1ekQKwPchEN_fO4AK0eyDd_JID5YO3hAF"

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
        PASTA_RAIZ = "1ekQKwPchEN_fO4AK0eyDd_JID5YO3hAF"

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

          

        # Salvar também o JSON com prefixo IA_
        dados_json = {
            "titulo": "MICROAMBIENTE DE EQUIPES - DIMENSÕES",
            "subtitulo": f"{empresa} / {emailLider} / {codrodada} / {data_hora}",
            "dados": resultado[["DIMENSAO", "IDEAL_%", "REAL_%"]].to_dict(orient="records")
        }
        salvar_json_ia_no_drive(dados_json, nome_pdf, service, id_lider)

        os.remove(caminho_pdf)
        return jsonify({"mensagem": "✅ Gráfico gerado e salvo no Drive com sucesso."})


    except Exception as e:
        import traceback
        print("❌ ERRO NA ROTA /salvar-grafico-autoavaliacao:")
        traceback.print_exc()
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


        
        # Salvar também o JSON com prefixo IA_
        dados_json = {
            "titulo": "MICROAMBIENTE DE EQUIPES - SUBDIMENSÕES",
            "subtitulo": f"{empresa} / {emailLider} / {codrodada} / {data_hora}",
            "dados": resultado[["SUBDIMENSAO", "IDEAL_%", "REAL_%"]].to_dict(orient="records")
        }
        salvar_json_ia_no_drive(dados_json, nome_pdf, service, id_lider)

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

        # Salvar também o JSON com prefixo IA_ na subpasta ia_json
        dados_json = {
            "titulo": "MÉDIA DA EQUIPE - DIMENSÕES",
            "subtitulo": f"{empresa} / {emailLider} / {codrodada} / {data_hora}",
            "dados": resultado[["DIMENSAO", "IDEAL_%", "REAL_%"]].to_dict(orient="records")
        }
        salvar_json_ia_no_drive(dados_json, nome_pdf, service, id_lider)

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

        # Salvar também o JSON com prefixo IA_ na subpasta ia_json
        dados_json = {
            "titulo": "MICROAMBIENTE DE EQUIPES - SUBDIMENSÕES",
            "subtitulo": f"{empresa} / {emailLider} / {codrodada} / {data_hora}",
            "dados": resultado[["SUBDIMENSAO", "IDEAL_%", "REAL_%"]].to_dict(orient="records")
        }
        salvar_json_ia_no_drive(dados_json, nome_pdf, service, id_lider)

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

        # Salvar também o JSON com prefixo IA_ na subpasta ia_json
        dados_json = {
            "titulo": "GAP MÉDIO POR DIMENSÃO E SUBDIMENSÃO",
            "subtitulo": f"{empresa} / {emailLider} / {codrodada} / {pd.Timestamp.now().strftime('%d/%m/%Y')}",
            "dados": {
                "dimensao": gap_dim.to_dict(orient="records"),
                "subdimensao": gap_subdim.to_dict(orient="records")
            }
        }
        salvar_json_ia_no_drive(dados_json, nome_arquivo, service, id_lider)

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



