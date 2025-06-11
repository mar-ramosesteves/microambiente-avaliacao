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
