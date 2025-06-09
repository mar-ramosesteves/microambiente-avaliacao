from flask import Flask, request, jsonify
import pandas as pd

app = Flask(__name__)

# Carrega as planilhas auxiliares
tabela_dim = pd.read_excel("pontos_maximos_dimensao.xlsx")
tabela_sub = pd.read_excel("pontos_maximos_subdimensao.xlsx")
matriz = pd.read_excel("TABELA_GERAL_MICROAMBIENTE_COM_CHAVE.xlsx")

# Extrai apenas uma linha por afirmação para relacionar chave com dimensão/subdimensão
afirmacoes = matriz.drop_duplicates(subset=["AFIRMACAO", "SUBDIMENSAO", "DIMENSAO"])[["CHAVE", "DIMENSAO", "SUBDIMENSAO"]]

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
            valor = int(valor)
            if not (1 <= valor <= 6):
                continue
        except:
            continue

        tipo = "IDEAL" if "_ideal" in chave.lower() else "REAL"
        cod = chave.replace("_ideal", "").replace("_real", "").replace("_IDEAL", "").replace("_REAL", "")
        chave_matriz = f"{cod}_{tipo}"


        linha = afirmacoes[afirmacoes["CHAVE"] == chave_matriz]
        if linha.empty:
            continue

        dim = linha.iloc[0]["DIMENSAO"]
        sub = linha.iloc[0]["SUBDIMENSAO"]

        if valor == 6:
            pontos = 2
        elif valor == 5:
            pontos = 1.5
        elif valor == 4:
            pontos = 1
        else:
            pontos = 0

        acumulado.setdefault(sub, {}).setdefault(tipo, 0)
        acumulado[sub][tipo] += pontos

    # Percentuais por subdimensão
    resultado_sub = {}
    for _, row in tabela_sub.iterrows():
        sub = row["SUBDIMENSAO"]
        max_pontos = row["PONTOS_MAXIMOS_SUBDIMENSAO"]
        ideal = acumulado.get(sub, {}).get("IDEAL", 0)
        real = acumulado.get(sub, {}).get("REAL", 0)
        resultado_sub[sub] = {
            "ideal": round((ideal / max_pontos) * 100, 1) if max_pontos else 0,
            "real": round((real / max_pontos) * 100, 1) if max_pontos else 0
        }

    # Percentuais por dimensão
    resultado_dim = {}
    for _, row in tabela_dim.iterrows():
        dim = row["DIMENSAO"]
        subdimensoes = tabela_sub[tabela_sub["DIMENSAO"] == dim]["SUBDIMENSAO"]
        total_ideal = sum(acumulado.get(sub, {}).get("IDEAL", 0) for sub in subdimensoes)
        total_real = sum(acumulado.get(sub, {}).get("REAL", 0) for sub in subdimensoes)
        max_dim = row["PONTOS_MAXIMOS_DIMENSAO"]
        resultado_dim[dim] = {
            "ideal": round((total_ideal / max_dim) * 100, 1) if max_dim else 0,
            "real": round((total_real / max_dim) * 100, 1) if max_dim else 0
        }

    return jsonify({
        "dimensoes": resultado_dim,
        "subdimensoes": resultado_sub
    })

if __name__ == "__main__":
    app.run(debug=True)
