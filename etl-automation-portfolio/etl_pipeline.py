"""
Script: Processamento de Dados Firebird para Google Sheets

Descrição:
Este script realiza a extração de dados de vendas, estoque e financeiros de um
banco de dados Firebird, realiza o tratamento, cálculo de KPIs e inteligência de negócios,
fazendo o upload performático dos resultados para dashboards do Google Sheets.

Tecnologias:
- Python
- Pandas
- FDB (Firebird)
- Gspread (Google Sheets API)
- Dotenv (Variáveis de Ambiente)
- Logging

Autor: João Otávio Mota Roriz
Projeto: Python Automation Portfolio
Contato: https://www.linkedin.com/in/joaootaviororiz/ | https://github.com/joaootaviororiz
"""

import os
import sys
import time
import logging

from dotenv import load_dotenv

# Configuração de Logs
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Deleta logs com mais de 7 dias
now = time.time()
for f in os.listdir(log_dir):
    f_path = os.path.join(log_dir, f)
    if os.stat(f_path).st_mtime < now - 7 * 86400:
        try: os.remove(f_path)
        except: pass

data_atual = time.strftime('%Y-%m-%d')
log_file = os.path.join(log_dir, f"automation_{data_atual}.log")

log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
log_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
log_handler.setFormatter(log_formatter)
log_handler.setLevel(logging.INFO)

app_log = logging.getLogger('root')
app_log.setLevel(logging.INFO)
app_log.addHandler(log_handler)

# Adiciona ao console também
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)
app_log.addHandler(console_handler)

# Carrega as variáveis de ambiente
load_dotenv()

import pandas as pd
import locale
import fdb # Driver do Firebird
import gspread # Comunicação com Google Sheets
from gspread_dataframe import get_as_dataframe # Função auxiliar para ler DataFrame
from gspread_dataframe import set_with_dataframe # Função auxiliar para escrever DataFrame
from datetime import datetime
from dateutil.relativedelta import relativedelta
from oauth2client.service_account import ServiceAccountCredentials # Para autenticação

# =======================================================================
# --- 1. CONFIGURAÇÕES DO FIREBIRD E GOOGLE SHEETS ---
# =======================================================================
FIREBIRD_HOST = os.getenv('FIREBIRD_HOST')
FIREBIRD_DB = os.getenv('FIREBIRD_DB')
FIREBIRD_USER = os.getenv('FIREBIRD_USER')
FIREBIRD_PASS = os.getenv('FIREBIRD_PASS')
FIREBIRD_CHARSET = os.getenv('FIREBIRD_CHARSET', 'WIN1252')

GOOGLE_CREDENTIALS_FILE = os.getenv('GOOGLE_CREDENTIALS_FILE')
PLANILHA_ID = os.getenv('PLANILHA_ID')
ABA_NOME = 'Vendas_Input'


def conectar_firebird():
    """
    Estabelece e devolve uma conexão com o banco de dados Firebird 
    usando as credenciais (variáveis de ambiente) predefinidas centralmente no topo.
    """
    return fdb.connect(
        host=FIREBIRD_HOST,
        database=FIREBIRD_DB,
        user=FIREBIRD_USER,
        password=FIREBIRD_PASS,
        charset=FIREBIRD_CHARSET
    )


# Define a janela de atualização: Mês Atual e Mês Anterior.
PRIMEIRO_DIA_MES_ANTERIOR = (datetime.today() - relativedelta(months=1)).replace(day=1)
#DATA_INICIO_JANELA = PRIMEIRO_DIA_MES_ANTERIOR.strftime('%m/%d/%Y') # Formato Firebird (dia.mês.ano)
DATA_INICIO_JANELA = '01/01/2024' # formato fixo para testes MM/DD/YYYY para o Firebird.


# =======================================================================
# --- 2. FUNÇÃO DE EXTRAÇÃO DO FIREBIRD ---
# =======================================================================
def extrair_dados_firebird():
    """
    Conecta ao Firebird e extrai dados de vendas, resumidos por Vendedor e Mês/Ano,
    limitando-se à Janela de Atualização definida (Mês Atual + Mês Anterior).
    """
    conn = None
    df = pd.DataFrame()
    
    # A consulta deve trazer:
    # 1. Uma CHAVE_MES_ANO para identificar o mês
    # 2. A VENDEDOR para identificar o agente
    # 3. As métricas de vendas (Valor, Quantidade, Êxito)
    QUERY_SQL = f"""
        SELECT DISTINCT 
            M.CODFILIAL,
            M.IDMOV,
            M.DATAEMISSAO,
            M.HORARIOEMISSAO,
            M.CODCFO,
            C.NOMEFANTASIA AS CLIFANTASIA,
            C.NOME AS CLIRAZAO,
            T.NOME AS NOMETIPOMOV,
            -- Valores Bruto (Corrigido para Devolução/Ajuste)
            CASE 
                WHEN SUBSTRING(M.CODTMV FROM 1 FOR 4) IN ('2.3.', '1.3.') THEN M.VALORBRUTO * -1
                ELSE M.VALORBRUTO
            END AS VALORBRUTO,
            -- Valores Líquido (Corrigido para Devolução/Ajuste)
            CASE 
                WHEN SUBSTRING(M.CODTMV FROM 1 FOR 4) IN ('2.3.', '1.3.') THEN M.VALORLIQUIDO * -1
                ELSE M.VALORLIQUIDO
            END AS VALORLIQUIDO,
            -- Descontos (Corrigido para Devolução/Ajuste)
            CASE 
                WHEN SUBSTRING(M.CODTMV FROM 1 FOR 4) IN ('2.3.', '1.3.') THEN 
                    (COALESCE(M.VALORDESC, 0) + COALESCE(M.VALORDESCSERV, 0)) * -1
                ELSE 
                    COALESCE(M.VALORDESC, 0) + COALESCE(M.VALORDESCSERV, 0)
            END AS VALORDESC,
            -- Desconto de Itens (Corrigido para Devolução/Ajuste)
            CASE 
                WHEN SUBSTRING(M.CODTMV FROM 1 FOR 4) IN ('2.3.', '1.3.') THEN M.VALORDESCITENS * -1
                ELSE M.VALORDESCITENS
            END AS VALORDESCITENS,    
            -- Quantidade Total de Itens (Corrigido para Devolução/Ajuste)
            (
                SELECT SUM(
                    CASE 
                        WHEN SUBSTRING(M.CODTMV FROM 1 FOR 4) IN ('2.3.', '1.3.') THEN I.QUANTIDADE * -1
                        ELSE I.QUANTIDADE
                    END
                )
                FROM TMOVITENS I
                WHERE I.IDMOV = M.IDMOV
                AND I.CODEMPRESA = M.CODEMPRESA
            ) AS QTDITEM,
            M.CODTMV,
            M.STATUS,
            M.STATUSPEDIDO,
            F.NOME AS NOMEFILIAL,
            M.CODUSUARIO,
            V.NOME AS NOMEVENDEDOR,
            CP.AVISTA,
            CP.NOME AS NOMECONDICAO,
            M.PERCENTUALDESC,
            -- Chave de Mês/Ano para a lógica de atualização no Sheets
            EXTRACT(MONTH FROM M.DATAEMISSAO) || '/' || EXTRACT(YEAR FROM M.DATAEMISSAO) AS MESANO 
        FROM TMOV M
            LEFT JOIN TTIPOMOV T 
                ON T.CODEMPRESA = M.CODEMPRESA 
            AND T.CODTIPOMOV = M.CODTMV
            LEFT JOIN FCFO C 
                ON M.CODEMPRESA = C.CODEMPRESA 
            AND M.CODCFO = C.CODCFO
            LEFT JOIN GFILIAL F 
                ON M.CODEMPRESA = F.CODEMPRESA 
            AND F.CODFILIAL = M.CODFILIAL
            LEFT JOIN TVENDEDOR V 
                ON M.CODEMPRESA = V.CODEMPRESA 
            AND M.CODVEN1 = V.CODVEN
            LEFT JOIN TCONDPGTO CP 
                ON M.CODEMPRESA = CP.CODEMPRESA 
            AND M.CODCPG = CP.CODCONDPGTO
            LEFT JOIN TREP R 
                ON M.CODEMPRESA = R.CODEMPRESA 
            AND M.CODRPR = R.CODREP
            LEFT JOIN FPORTADOR PO  
                ON PO.CODEMPRESA = M.CODEMPRESA 
            AND PO.COD = M.CODPORTADOR
        WHERE 
            M.CODTMV IN ('2.2.04', '2.3.03', '2.3.04')
            AND M.CODFILIAL BETWEEN '1' AND '2'
            -- AJUSTE CRÍTICO: Usa a variável dinâmica calculada no Python
            AND M.DATAEMISSAO >= '{DATA_INICIO_JANELA}' 
            AND M.STATUS <> 'C'
            AND M.CODEMPRESA = 1
        ORDER BY 
            M.HORARIOEMISSAO
        """
    app_log.info(f"Buscando dados no Firebird desde: {DATA_INICIO_JANELA}")

    try:
        conn = conectar_firebird()
        cursor = conn.cursor()
        cursor.execute(QUERY_SQL)
        
        colunas = [i[0] for i in cursor.description]
        dados = cursor.fetchall()
        df = pd.DataFrame(dados, columns=colunas)
        
        app_log.info(f"Extração do Firebird concluída. {len(df)} registros encontrados.")
    except Exception as e:
        app_log.error(f"ERRO DE FIREBIRD: Falha ao conectar ou extrair dados: {e}")
    finally:
        if conn:
            conn.close()
    return df

# =======================================================================
# --- 3. FUNÇÕES DE GOOGLE SHEETS (gspread) ---
# =======================================================================

def autenticar_sheets():
    """Autentica o gspread usando o arquivo de credenciais."""
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        # Usa o arquivo de credenciais que você forneceu
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        app_log.error(f"ERRO DE AUTENTICAÇÃO: Verifique o arquivo de credenciais '{GOOGLE_CREDENTIALS_FILE}'. Erro: {e}")
        return None

def ler_historico_do_sheets():
    """Lê todo o conteúdo da aba de vendas para um DataFrame do Pandas."""
    try:
        client = autenticar_sheets()
        if not client:
            return pd.DataFrame()
            
        sheet = client.open_by_key(PLANILHA_ID).worksheet(ABA_NOME)
        dados = sheet.get_all_records()
        
        if dados:
            df = pd.DataFrame(dados)
            
            # 🚨 CORREÇÃO CRÍTICA AQUI 🚨
            # Garante que MESANO seja lido como string para que o filtro funcione.
            if 'MESANO' in df.columns:
                df['MESANO'] = df['MESANO'].astype(str)
                
            # Não precisamos de DATAEMISSAO como datetime aqui, apenas MESANO!
            app_log.info(f"Histórico do Sheets lido. {len(df)} registros encontrados.")
            return df
        else:
            return pd.DataFrame(columns=['DATAEMISSAO', 'MESANO']) 
            
    except Exception as e:
        app_log.error(f"ERRO AO LER HISTÓRICO DO SHEETS: {e}")
        return pd.DataFrame()

def sobrescrever_a_planilha(df_final):
    """Limpa a aba de Vendas e escreve o DataFrame final (histórico + novos dados)."""
    if df_final.empty:
        app_log.info("DataFrame final vazio. Nada para sobrescrever.")
        return
        
    try:
        client = autenticar_sheets()
        if not client:
            return
            
        sheet = client.open_by_key(PLANILHA_ID).worksheet(ABA_NOME)

        # Usando set_with_dataframe para escrever o DataFrame de volta (inclui o cabeçalho)
        set_with_dataframe(sheet, df_final, row=1, col=1, include_index=False, resize=True)
        
        app_log.info(f"Sucesso: Planilha '{ABA_NOME}' sobrescrita com {len(df_final)} registros.")

    except Exception as e:
        app_log.error(f"ERRO AO ESCREVER NO GOOGLE SHEETS: {e}")
        
# =======================================================================
# --- 4. ATUALIZAÇÃO DA LISTA DE VENDEDORES ATIVOS NO SHEETS ---
# =======================================================================

def atualizar_lista_vendedores():
    
    app_log.info("Atualizando lista de vendedores ativos...")

    SQL_VENDEDORES = """
        SELECT CODVEN, NOME, CARGO, CODFILIAL, INATIVO
        FROM TVENDEDOR
        WHERE CODEMPRESA = 1
          AND (INATIVO = 'F' OR INATIVO IS NULL)
          AND CODVEN NOT IN (143,121,23,5)
        ORDER BY NOME
    """

    # ---- 1. Extrair vendedores do Firebird ----
    conn = None
    try:
        conn = conectar_firebird()
        cursor = conn.cursor()
        cursor.execute(SQL_VENDEDORES)

        colunas = [c[0] for c in cursor.description]
        dados = cursor.fetchall()
        df_vendedores = pd.DataFrame(dados, columns=colunas)

        app_log.info(f"{len(df_vendedores)} vendedores ativos encontrados.")

    except Exception as e:
        app_log.error(f"ERRO AO BUSCAR VENDEDORES: {e}")
        return

    finally:
        if conn:
            conn.close()

    # ---- 2. Enviar para o Google Sheets ----
    try:
        client = autenticar_sheets()
        if not client:
            app_log.info("Falha ao autenticar Sheets.")
            return

        try:
            aba = client.open_by_key(PLANILHA_ID).worksheet("Lista_Vendedores")
        except:
            # Caso não exista, cria a aba
            aba = client.open_by_key(PLANILHA_ID).add_worksheet("Lista_Vendedores", rows=500, cols=10)

        # Reescreve a aba inteira com o DataFrame
        set_with_dataframe(aba, df_vendedores, row=1, col=1, include_index=False, resize=True)

        app_log.info("Lista de vendedores atualizada com sucesso.")

    except Exception as e:
        app_log.error(f"ERRO AO ESCREVER LISTA DE VENDEDORES NO SHEETS: {e}")
        
# ============================
# INTEGRAÇÃO: Clientes_Historico
# (cole este bloco dentro do seu ETL, mantendo seus imports e variáveis existentes)
# ============================
def gerar_clientes_historico():
    """
    Executa uma consulta no Firebird que retorna:
      - CODCFO
      - CLIRAZAO
      - DATA_PRIMEIRA_COMPRA
      - DATA_ULTIMA_COMPRA
      - NUM_COMPRAS
      - TICKET_MEDIO
      - TIPO_CLIENTE (NOVO, CLIENTE ATIVO, CLIENTE ANTIGO, REATIVADO)
    Retorna um pandas.DataFrame pronto para ser enviado ao Sheets.
    """
    # SQL pensado para Firebird (mantendo formato de datas e funções compatíveis).
    SQL_CLIENTES = f"""
            WITH AGREGADO AS (
            SELECT
                C.CODCFO,
                C.NOME AS CLIRAZAO,
                MIN(M.DATAEMISSAO) AS DATA_PRIMEIRA_COMPRA,
                MAX(M.DATAEMISSAO) AS DATA_ULTIMA_COMPRA,
                COUNT(*) AS NUM_COMPRAS,
                AVG(
                    CASE 
                        WHEN SUBSTRING(M.CODTMV FROM 1 FOR 4) IN ('2.3.', '1.3.') 
                            THEN M.VALORLIQUIDO * -1
                        ELSE M.VALORLIQUIDO
                    END
                ) AS TICKET_MEDIO
            FROM TMOV M
            JOIN FCFO C 
                ON C.CODEMPRESA = M.CODEMPRESA
                AND C.CODCFO = M.CODCFO
            WHERE
                M.CODTMV IN ('2.2.04','2.3.03','2.3.04')
                AND M.STATUS <> 'C'
                AND m.CODCFO <> 'C00000' 
            GROUP BY
                C.CODCFO,
                C.NOME
        ),
        ULTIMA_COMPRA_ANTERIOR AS (
            SELECT
                A.CODCFO,
                (
                    SELECT MAX(M2.DATAEMISSAO)
                    FROM TMOV M2
                    WHERE 
                        M2.CODCFO = A.CODCFO
                        AND M2.DATAEMISSAO < A.DATA_ULTIMA_COMPRA
                ) AS DATA_PENULTIMA_COMPRA
            FROM AGREGADO A
        )
        SELECT
            A.*,
            U.DATA_PENULTIMA_COMPRA,
            CASE
                WHEN A.DATA_PRIMEIRA_COMPRA >= '{DATA_INICIO_JANELA}' THEN 'CLIENTE NOVO'
                WHEN 
                    U.DATA_PENULTIMA_COMPRA IS NOT NULL
                    AND (A.DATA_ULTIMA_COMPRA - U.DATA_PENULTIMA_COMPRA) > 90
                    AND A.DATA_ULTIMA_COMPRA >= DATEADD(DAY, -90, CURRENT_DATE)
                THEN 'CLIENTE REATIVADO'
                WHEN A.DATA_ULTIMA_COMPRA >= DATEADD(DAY, -90, CURRENT_DATE)
                THEN 'CLIENTE ATIVO'
                WHEN 
                    A.DATA_ULTIMA_COMPRA < DATEADD(DAY, -90, CURRENT_DATE)
                    AND A.DATA_ULTIMA_COMPRA >= DATEADD(DAY, -180, CURRENT_DATE)
                THEN 'CLIENTE EM RISCO'
                WHEN A.DATA_ULTIMA_COMPRA < DATEADD(DAY, -180, CURRENT_DATE)
                THEN 'CLIENTE PERDIDO'
                ELSE 'NÃO CLASSIFICADO'
            END AS TIPO_CLIENTE
        FROM AGREGADO A
        LEFT JOIN ULTIMA_COMPRA_ANTERIOR U ON U.CODCFO = A.CODCFO
        ORDER BY A.CLIRAZAO;
    """

    # Tentativa de execução no Firebird
    df = pd.DataFrame()
    conn = None
    try:
        conn = conectar_firebird()
        cursor = conn.cursor()
        cursor.execute(SQL_CLIENTES)
        cols = [c[0] for c in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=cols)

        # ------------------------------------------------------------
        # 1) Padroniza datas como STRING YYYY-MM-DD
        # ------------------------------------------------------------
        for col in ["DATA_PRIMEIRA_COMPRA", "DATA_ULTIMA_COMPRA"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else str(x)
                )

        # ------------------------------------------------------------
        # 2) Converte string → Timestamp (somente para comparações)
        # ------------------------------------------------------------
        if "DATA_PRIMEIRA_COMPRA" in df.columns:
            df["DATA_PRIMEIRA_COMPRA"] = pd.to_datetime(df["DATA_PRIMEIRA_COMPRA"], errors="coerce")
        if "DATA_ULTIMA_COMPRA" in df.columns:
            df["DATA_ULTIMA_COMPRA"] = pd.to_datetime(df["DATA_ULTIMA_COMPRA"], errors="coerce")

        # Ticket e número de compras
        if "TICKET_MEDIO" in df.columns:
            df["TICKET_MEDIO"] = pd.to_numeric(df["TICKET_MEDIO"], errors="coerce")
        if "NUM_COMPRAS" in df.columns:
            df["NUM_COMPRAS"] = pd.to_numeric(df["NUM_COMPRAS"], errors="coerce").astype("Int64")

        # ------------------------------------------------------------
        # 3) Se PREV_DATA não vier do SQL → calcular via segunda consulta
        # ------------------------------------------------------------
        if "PREV_DATA" not in df.columns or df["PREV_DATA"].isnull().all():

            sql_prev = """
                SELECT T1.CODCFO, MAX(T1.DATAEMISSAO) AS PREV_DATE
                FROM TMOV T1
                JOIN (
                    SELECT CODCFO, MAX(DATAEMISSAO) AS MAX_DATE
                    FROM TMOV
                    WHERE CODTMV IN ('2.2.04','2.3.03','2.3.04')
                    AND STATUS <> 'C'
                    AND CODEMPRESA = 1
                    GROUP BY CODCFO
                ) T2 ON T1.CODCFO = T2.CODCFO
                WHERE T1.DATAEMISSAO < T2.MAX_DATE
                GROUP BY T1.CODCFO
            """

            prev_dates = {}
            try:
                cursor.execute(sql_prev)
                rows_prev = cursor.fetchall()
                for r in rows_prev:
                    prev_dates[r[0]] = pd.to_datetime(r[1], errors="coerce")
            except:
                prev_dates = {}

            df["PREV_DATA"] = df["CODCFO"].map(prev_dates)

        # ------------------------------------------------------------
        # 4) Cálculo TIPO_CLIENTE (sempre seguro)
        # ------------------------------------------------------------
        inicio_janela = pd.to_datetime(PRIMEIRO_DIA_MES_ANTERIOR)
        hoje = pd.Timestamp.today().normalize()

        def classify(row):
            primeira = row["DATA_PRIMEIRA_COMPRA"]
            ultima = row["DATA_ULTIMA_COMPRA"]
            prev = row.get("PREV_DATA", pd.NaT)

            if pd.isna(primeira):
                return "NÃO CLASSIFICADO"

            # → NOVO CLIENTE
            if primeira >= inicio_janela:
                return "CLIENTE NOVO"

            # → REATIVADO (gap > 90 e voltou nos últimos 90 dias)
            if not pd.isna(prev) and not pd.isna(ultima):
                if (ultima - prev).days > 90 and (hoje - ultima).days <= 90:
                    return "CLIENTE REATIVADO"

            # Dias sem comprar
            if pd.isna(ultima):
                return "NÃO CLASSIFICADO"

            dias = (hoje - ultima).days

            # → ATIVO (≤ 90 dias)
            if dias <= 90:
                return "CLIENTE ATIVO"

            # → EM RISCO (entre 91 e 180)
            if 90 < dias <= 180:
                return "CLIENTE EM RISCO"

            # → PERDIDO (> 180)
            if dias > 180:
                return "CLIENTE PERDIDO"

            return "NÃO CLASSIFICADO"
        df["TIPO_CLIENTE"] = df.apply(classify, axis=1)


        # ------------------------------------------------------------
        # 5) Reordena colunas
        # ------------------------------------------------------------
        cols_order = [
            "CODCFO", "CLIRAZAO",
            "DATA_PRIMEIRA_COMPRA", "DATA_ULTIMA_COMPRA",
            "TIPO_CLIENTE",
            "TICKET_MEDIO", "NUM_COMPRAS"
        ]
        df = df[[c for c in cols_order if c in df.columns]]

        # ------------------------------------------------------------
        # 6) Converte datas de volta para STRING antes de enviar ao Sheets
        # ------------------------------------------------------------
        for col in ["DATA_PRIMEIRA_COMPRA", "DATA_ULTIMA_COMPRA"]:
            if col in df.columns:
                df[col] = df[col].dt.strftime("%Y-%m-%d")

        return df

    except Exception as e:
        app_log.error(f"ERRO ao gerar Clientes_Historico: {e}")
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()



def atualizar_clientes_historico():
    """
    Roda a query de clientes e sobrescreve a aba 'Clientes_Historico' no Google Sheets.
    Não altera outras abas.
    """
    app_log.info("Atualizando Clientes_Historico...")

    df_clientes = gerar_clientes_historico()
    if df_clientes.empty:
        app_log.info("Nenhum registro para Clientes_Historico. Abortando atualização.")
        return

    try:
        client = autenticar_sheets()
        if not client:
            app_log.info("Falha ao autenticar Sheets para Clientes_Historico.")
            return

        # Cria ou abre aba
        sh = client.open_by_key(PLANILHA_ID)
        try:
            aba = sh.worksheet("Clientes_Historico")
        except Exception:
            aba = sh.add_worksheet(title="Clientes_Historico", rows=1000, cols=20)

        # Escrever (sobrescrever) com set_with_dataframe
        set_with_dataframe(aba, df_clientes, row=1, col=1, include_index=False, resize=True)
        app_log.info(f"Clientes_Historico atualizado: {len(df_clientes)} registros gravados.")

    except Exception as e:
        app_log.error(f"ERRO AO ESCREVER CLIENTES_HISTORICO NO SHEETS: {e}")


def gerar_kpis_clientes_mensal():
    """
    Gera KPIs mensais de clientes por TIPO_CLIENTE
    """
    app_log.info("Gerando KPIs mensais de clientes...")

    df = gerar_clientes_historico()
    if df.empty:
        return pd.DataFrame()

    # Garantir datas
    df["DATA_ULTIMA_COMPRA"] = pd.to_datetime(df["DATA_ULTIMA_COMPRA"], errors="coerce")

    # Mês da última compra
    df["MES_REF"] = df["DATA_ULTIMA_COMPRA"].dt.to_period("M").astype(str)

    kpi_mensal = (
        df.groupby(["MES_REF", "TIPO_CLIENTE"])["CODCFO"]
        .nunique()
        .reset_index(name="QTD_CLIENTES")
    )

    kpi_mensal["PERIODO"] = kpi_mensal["MES_REF"]

    return kpi_mensal[["PERIODO", "TIPO_CLIENTE", "QTD_CLIENTES"]]

        
def gerar_kpis_clientes_3m():
    """
    Gera KPIs acumulados dos últimos 3 meses
    """
    app_log.info("Gerando KPIs acumulados 3M...")

    df = gerar_clientes_historico()
    if df.empty:
        return pd.DataFrame()

    hoje = pd.Timestamp.today().normalize()
    limite_3m = hoje - pd.DateOffset(months=3)

    df["DATA_ULTIMA_COMPRA"] = pd.to_datetime(df["DATA_ULTIMA_COMPRA"], errors="coerce")

    df_3m = df[df["DATA_ULTIMA_COMPRA"] >= limite_3m]

    kpi_3m = (
        df_3m.groupby("TIPO_CLIENTE")["CODCFO"]
        .nunique()
        .reset_index(name="QTD_CLIENTES")
    )

    kpi_3m["PERIODO"] = "3M"

    return kpi_3m[["PERIODO", "TIPO_CLIENTE", "QTD_CLIENTES"]]


def gerar_kpis_ciclo_clientes():
    mensal = gerar_kpis_clientes_mensal()
    tres_meses = gerar_kpis_clientes_3m()

    df_final = pd.concat([mensal, tres_meses], ignore_index=True)
    return df_final


def atualizar_kpis_ciclo_clientes():
    app_log.info("Atualizando KPIs_Ciclo_Clientes...")

    df_kpis = gerar_kpis_ciclo_clientes()
    if df_kpis.empty:
        app_log.info("Nenhum KPI gerado.")
        return

    client = autenticar_sheets()
    sh = client.open_by_key(PLANILHA_ID)

    try:
        aba = sh.worksheet("KPIs_Ciclo_Clientes")
    except:
        aba = sh.add_worksheet(title="KPIs_Ciclo_Clientes", rows=1000, cols=10)

    set_with_dataframe(aba, df_kpis, include_index=False, resize=True)
    app_log.info("KPIs_Ciclo_Clientes atualizado com sucesso.")
    
    
def gerar_financeiro_titulos():
    """
    Extrai títulos financeiros (Pagar e Receber) desde 2023
    para alimentar o Looker.
    """

    SQL_FINANCEIRO = """
        SELECT
            FLAN.IDLAN,
            FLAN.CODFILIAL,
            GFILIAL.NOMEFANTASIA AS FILIAL,
            CASE 
                WHEN FLAN.PAGREC = 'R' THEN 'RECEBER'
                ELSE 'PAGAR'
            END AS TIPO_DESCRICAO, -- R = Receber | P = Pagar
            --FLAN.PAGREC AS TIPO,
            FLAN.CODCFO,
            FCFO.NOMEFANTASIA AS CLIENTE_FORNECEDOR,
            CASE 
                WHEN FCFO.CODTIPOCFO = '3' THEN 'FUNCIONARIO'
                ELSE 'CLIENTE'
            END AS TIPO_CLIENTE,
            FLAN.PARCELA,
            FLAN.DATAEMISSAO,
            FLAN.DATAVENCIMENTO,
            FLAN.DATABAIXA,
            FLAN.VALORORIGINAL,
            FLAN.VALORBAIXADO,
            (FLAN.VALORORIGINAL - COALESCE(FLAN.VALORBAIXADO,0)) AS SALDO_ABERTO,
            CASE
                WHEN FLAN.DATABAIXA IS NOT NULL 
                    OR FLAN.IDBAIXAPARCIAL IS NOT NULL THEN 'PAGO'
                WHEN FLAN.DATAVENCIMENTO < CURRENT_DATE THEN 'VENCIDO'
                ELSE 'EM ABERTO'
            END AS STATUS_FINANCEIRO
        FROM FLAN
        LEFT JOIN FCFO
            ON FCFO.CODCFO = FLAN.CODCFO
            AND FCFO.CODEMPRESA = FLAN.CODEMPRESA
        LEFT JOIN GFILIAL
            ON GFILIAL.CODFILIAL = FLAN.CODFILIAL
            AND GFILIAL.CODEMPRESA = FLAN.CODEMPRESA            
		LEFT JOIN FTIPODOC 
			ON FTIPODOC.CODTIPODOC=FLAN.CODTDO 
			AND FTIPODOC.CODEMPRESA=FLAN.CODEMPRESA
        WHERE
        	FLAN.CODEMPRESA = 1 
            AND FLAN.DATAVENCIMENTO >= DATE '2023-01-01'
            AND ((FLAN.PAGREC='R') OR (FLAN.PAGREC='P')) 
            AND (FLAN.STATUSLAN  IN ('A','B')) 
            AND (Coalesce(FTIPODOC.CLASSIFICACAO,'') <> 'P') 
            AND (Coalesce(FTIPODOC.CLASSIFICACAO,'') <> 'V') 
            AND (Coalesce(FLAN.INATIVO,'F')='F') 
            AND ((FTIPODOC.CLASSIFICACAO<>'A' OR FTIPODOC.CLASSIFICACAO IS NULL) OR (FLAN.IDBAIXAPARCIAL IS NULL OR FLAN.IDBAIXAPARCIAL = 0)) 
            ORDER BY FLAN.DATAVENCIMENTO;
    """

    df = pd.DataFrame()
    conn = None

    try:
        conn = conectar_firebird()

        cursor = conn.cursor()
        cursor.execute(SQL_FINANCEIRO)

        cols = [c[0] for c in cursor.description]
        rows = cursor.fetchall()

        df = pd.DataFrame(rows, columns=cols)

        # Padroniza datas para o Sheets
        for col in ["DATAEMISSAO", "DATAVENCIMENTO", "DATABAIXA"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

        # Valores numéricos
        for col in ["VALORORIGINAL", "VALORBAIXADO", "SALDO_ABERTO"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    except Exception as e:
        app_log.error(f"ERRO ao gerar Financeiro_Titulos: {e}")
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()
            

def tratar_financeiro(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    cols_data = ["DATAEMISSAO", "DATAVENCIMENTO", "DATABAIXA"]

    # 1️⃣ Converter datas (garantir datetime)
    for col in cols_data:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    hoje = pd.Timestamp.today().normalize()

    # 2️⃣ Colunas derivadas (USANDO DATETIME)
    df["MES_ANO"] = df["DATAVENCIMENTO"].dt.to_period("M").astype(str)

    df["PERIODO"] = "FUTURO"
    df.loc[df["DATAVENCIMENTO"] < hoje, "PERIODO"] = "PASSADO"
    df.loc[df["DATAVENCIMENTO"] == hoje, "PERIODO"] = "HOJE"

    # 3️⃣ Formatação FINAL para Sheets (string)
    for col in cols_data:
        df[col] = df[col].dt.strftime("%d-%m-%Y")
        df[col] = df[col].fillna("")  # evita NaN / NaT no Sheets

    # 4️⃣ Limpeza de textos (importantíssimo pro Looker)
    cols_texto = df.select_dtypes(include="object").columns
    for col in cols_texto:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
        )
    return df
 
     
                           
def merge_incremental(df_novo: pd.DataFrame, df_sheets: pd.DataFrame) -> pd.DataFrame:
    df_novo = df_novo.set_index("IDLAN")
    df_sheets = df_sheets.set_index("IDLAN")

    # Atualiza registros existentes
    df_sheets.update(df_novo)

    # Insere novos registros
    novos = df_novo.loc[~df_novo.index.isin(df_sheets.index)]

    df_final = pd.concat([df_sheets, novos]).reset_index()
    return df_final

def calcular_semana_sab_sex(data):
    """
    Retorna a semana financeira no formato: 01-02-2026 a 07-02-2026 (sábado a sexta)
    """
    if pd.isna(data):
        return None

    data = pd.to_datetime(data)

    # weekday(): segunda=0 ... domingo=6
    dias_desde_sabado = (data.weekday() + 2) % 7
    inicio_semana = data - pd.Timedelta(days=dias_desde_sabado)
    fim_semana = inicio_semana + pd.Timedelta(days=6)

    return f"{inicio_semana.strftime('%d-%m-%Y')} a {fim_semana.strftime('%d-%m-%Y')}"


def classificar_bucket_semanal(data_vencimento, hoje):
    """
    Classifica o título em relação à semana financeira atual
    """
    if pd.isna(data_vencimento):
        return "SEM DATA"

    data_vencimento = pd.to_datetime(data_vencimento)

    # Semana atual
    dias_desde_sabado = (hoje.weekday() + 2) % 7
    inicio_semana_atual = hoje - pd.Timedelta(days=dias_desde_sabado)
    fim_semana_atual = inicio_semana_atual + pd.Timedelta(days=6)

    if data_vencimento < inicio_semana_atual:
        return "PASSADO"

    elif inicio_semana_atual <= data_vencimento <= fim_semana_atual:
        return "SEMANA ATUAL"

    elif data_vencimento <= fim_semana_atual + pd.Timedelta(days=7):
        return "SEMANA_1"

    elif data_vencimento <= fim_semana_atual + pd.Timedelta(days=14):
        return "SEMANA_2"

    elif data_vencimento <= fim_semana_atual + pd.Timedelta(days=21):
        return "SEMANA_3"

    else:
        return "FUTURO"


def recalcular_colunas_temporais(df):
    df = df.copy()
    hoje = pd.Timestamp.today().normalize()

    # 🔒 GARANTIA: converter só se ainda não for datetime
    if not pd.api.types.is_datetime64_any_dtype(df["DATAVENCIMENTO"]):
        df["DATAVENCIMENTO"] = pd.to_datetime(
            df["DATAVENCIMENTO"],
            format="%d-%m-%Y",
            errors="coerce"
        )

    # Semana financeira (sábado → sexta)
    df["SEMANA_FINANCEIRA"] = df["DATAVENCIMENTO"].apply(
        calcular_semana_sab_sex
    )

    # Bucket temporal
    df["BUCKET_TEMPO"] = df["DATAVENCIMENTO"].apply(
        lambda d: classificar_bucket_semanal(d, hoje)
    )

    # 🔁 Formatar novamente só no FINAL
    df["DATAVENCIMENTO"] = (
        df["DATAVENCIMENTO"]
        .dt.strftime("%d-%m-%Y")
        .fillna("")
    )

    return df



def atualizar_financeiro_titulos(backfill=True):
    app_log.info("Atualizando Financeiro_Titulos...")

    df_novo = gerar_financeiro_titulos()
    if df_novo.empty:
        app_log.info("Sem dados novos.")
        return
    
    if backfill:
        app_log.info("[BACKFILL] - Rodando BACKFILL financeiro (dados históricos)...")
        df_final = df_novo.copy()
    else:
        app_log.info("Atualização incremental financeira...")
        df_sheets = gerar_financeiro_titulos()
        df_final = df_sheets.copy()
        df_final.update(df_novo)
        novos = df_novo[~df_novo["IDLAN"].isin(df_sheets["IDLAN"])]
        df_final = pd.concat([df_final, novos], ignore_index=True)

    df_novo = tratar_financeiro(df_novo)
    
    
    client = autenticar_sheets()
    sh = client.open_by_key(PLANILHA_ID)

    try:
        aba = sh.worksheet("Financeiro_Titulos")
        df_sheets = get_as_dataframe(aba)

        if not df_sheets.empty:
            df_final = merge_incremental(df_novo, df_sheets)
        else:
            df_final = df_novo

    except Exception:
        aba = sh.add_worksheet(title="Financeiro_Titulos", rows=2000, cols=40)
        df_final = df_novo


    df_final = recalcular_colunas_temporais(df_final)
    
    app_log.info(
        f"Nulos: {df_final['DATAVENCIMENTO'].isna().sum()}, Vazios: {df_final['DATAVENCIMENTO'].eq('').sum()}"
    )

    set_with_dataframe(
        aba,
        df_final,
        row=1,
        col=1,
        include_index=False,
        resize=True
    )

    app_log.info(f"Financeiro_Titulos atualizado: {len(df_final)} registros.")






def gerar_vendas_produtos():
    SQL_VENDAS = """
        SELECT
          M.IDMOV,
          M.DATAEMISSAO,
          I.CODPRD,
          P.NOMEFANTASIA AS PRODUTO,
          G.DESCRICAO AS GRUPO,
          D.NOME AS DEPARTAMENTO,
          C.NOMEFANTASIA AS CLIENTE,
          V.NOME AS VENDEDOR,

          IIF(SUBSTRING(M.CODTMV FROM 1 FOR 4) IN ('2.3.','1.3.'),
              I.QUANTIDADE * -1,
              I.QUANTIDADE
          ) AS QUANTIDADE,

          I.PRECOUNITARIO,

          IIF(SUBSTRING(M.CODTMV FROM 1 FOR 4) IN ('2.3.','1.3.'),
              I.QUANTIDADE * I.PRECOUNITARIO * -1,
              I.QUANTIDADE * I.PRECOUNITARIO
          ) AS VALOR_TOTAL,

          P.CUSTOUNITARIO,
          P.CUSTOMEDIO

        FROM TMOV M
        LEFT JOIN TMOVITENS I ON (I.IDMOV=M.IDMOV AND I.CODEMPRESA=M.CODEMPRESA)
        LEFT JOIN TPRODUTO P ON (P.CODEMPRESA = I.CODEMPRESA AND P.CODPRD = I.CODPRD)
        LEFT JOIN TGRUPO G ON (P.CODGRUPO = G.CODGRUPO AND G.CODEMPRESA=P.CODEMPRESA)
        LEFT JOIN GDEPARTAMENTO D ON (D.CODDEPARTAMENTO = P.CODDEPARTAMENTO AND D.CODEMPRESA=P.CODEMPRESA)
        LEFT JOIN FCFO C ON (M.CODEMPRESA = C.CODEMPRESA AND M.CODCFO = C.CODCFO)
        LEFT JOIN TVENDEDOR V ON (V.CODVEN=M.CODVEN1 AND V.CODEMPRESA=M.CODEMPRESA)

        WHERE 
          M.CODEMPRESA = 1
          AND M.DATAEMISSAO >= DATEADD(-30 DAY TO CURRENT_DATE)
          AND M.CODFILIAL = '1'
          AND (M.STATUS IS NULL OR M.STATUS <> 'C')
          AND M.CODTMV IN ('2.2.04','2.3.03','2.3.04')
    """

    df = pd.DataFrame()
    conn = None

    try:
        conn = conectar_firebird()

        cursor = conn.cursor()
        cursor.execute(SQL_VENDAS)

        cols = [c[0] for c in cursor.description]
        rows = cursor.fetchall()

        df = pd.DataFrame(rows, columns=cols)

        # datas
        if "DATAEMISSAO" in df.columns:
            df["DATAEMISSAO"] = pd.to_datetime(df["DATAEMISSAO"], errors="coerce")

        # chave única (IMPORTANTE)
        df["ID_UNICO"] = df["IDMOV"].astype(str) + "_" + df["CODPRD"].astype(str)

        # numéricos
        for col in ["QUANTIDADE", "PRECOUNITARIO", "VALOR_TOTAL", "CUSTOUNITARIO", "CUSTOMEDIO"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    except Exception as e:
        app_log.error(f"ERRO ao gerar vendas_produtos: {e}")
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()
            
            
       
       
def atualizar_vendas_produtos():
    app_log.info("Atualizando vendas_produtos...")

    df_novo = gerar_vendas_produtos()

    if df_novo.empty:
        app_log.info("Sem dados de vendas.")
        return

    client = autenticar_sheets()
    sh = client.open_by_key(PLANILHA_ID)

    try:
        aba = sh.worksheet("vendas_produtos")
        df_sheets = get_as_dataframe(aba)

        if not df_sheets.empty:
            df_sheets["DATAEMISSAO"] = pd.to_datetime(df_sheets["DATAEMISSAO"], errors="coerce")

            # 🔥 remove últimos 30 dias antigos
            corte = pd.Timestamp.today() - pd.Timedelta(days=30)
            df_antigo = df_sheets[df_sheets["DATAEMISSAO"] < corte]

            # 🔥 junta com novos
            df_final = pd.concat([df_antigo, df_novo], ignore_index=True)

        else:
            df_final = df_novo

    except Exception:
        aba = sh.add_worksheet(title="vendas_produtos", rows=2000, cols=20)
        df_final = df_novo

    # formata data para sheets
    df_final["DATAEMISSAO"] = df_final["DATAEMISSAO"].dt.strftime("%Y-%m-%d")

    set_with_dataframe(
        aba,
        df_final,
        row=1,
        col=1,
        include_index=False,
        resize=True
    )

    app_log.info(f"Vendas atualizadas: {len(df_final)} registros.")
    
    
    
    
def gerar_estoque_produtos():
    SQL_ESTOQUE = """
        SELECT 
            P.CODPRD,
            P.NOMEFANTASIA AS PRODUTO,
            G.DESCRICAO AS GRUPO,
            F.NOME AS FABRICANTE,
            P.CUSTOUNITARIO,
            P.CUSTOMEDIO,
            P.SALDOGERALFISICO AS ESTOQUE
        FROM TPRODUTO P
            LEFT JOIN TGRUPO G 
                ON G.CODGRUPO = P.CODGRUPO 
                AND G.CODEMPRESA = P.CODEMPRESA
            LEFT JOIN GDEPARTAMENTO D 
                ON D.CODDEPARTAMENTO = P.CODDEPARTAMENTO
            LEFT JOIN TFAB F 
                ON F.CODFAB = P.CODFAB 
                AND F.CODEMPRESA = P.CODEMPRESA
        WHERE P.CODEMPRESA = 1
    """

    df = pd.DataFrame()
    conn = None

    try:
        conn = conectar_firebird()

        cursor = conn.cursor()
        cursor.execute(SQL_ESTOQUE)

        cols = [c[0] for c in cursor.description]
        rows = cursor.fetchall()

        df = pd.DataFrame(rows, columns=cols)

        # numéricos
        for col in ["ESTOQUE", "CUSTOUNITARIO", "CUSTOMEDIO"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    except Exception as e:
        app_log.error(f"ERRO ao gerar estoque_produtos: {e}")
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()
            
                    
     
def atualizar_estoque_produtos():
    app_log.info("Atualizando estoque_produtos...")

    df = gerar_estoque_produtos()

    if df.empty:
        app_log.info("Sem dados de estoque.")
        return

    client = autenticar_sheets()
    sh = client.open_by_key(PLANILHA_ID)

    try:
        aba = sh.worksheet("estoque_produtos")
    except Exception:
        aba = sh.add_worksheet(title="estoque_produtos", rows=2000, cols=10)

    set_with_dataframe(
        aba,
        df,
        row=1,
        col=1,
        include_index=False,
        resize=True
    )

    app_log.info(f"Estoque atualizado: {len(df)} registros.")




def gerar_inteligencia_produtos():
    app_log.info("Gerando inteligencia de produtos...")

    client = autenticar_sheets()
    sh = client.open_by_key(PLANILHA_ID)

    # =========================
    # 📥 CARREGA DADOS
    # =========================
    aba_vendas = sh.worksheet("vendas_produtos")
    aba_estoque = sh.worksheet("estoque_produtos")

    df_vendas = get_as_dataframe(aba_vendas)
    df_estoque = get_as_dataframe(aba_estoque)

    if df_vendas.empty or df_estoque.empty:
        app_log.info("Sem dados suficientes.")
        return pd.DataFrame()

    # =========================
    # 🔧 TRATAMENTOS
    # =========================
    df_vendas["DATAEMISSAO"] = pd.to_datetime(df_vendas["DATAEMISSAO"], errors="coerce")
    df_vendas["QUANTIDADE"] = pd.to_numeric(df_vendas["QUANTIDADE"], errors="coerce")

    df_estoque["ESTOQUE"] = pd.to_numeric(df_estoque["ESTOQUE"], errors="coerce")

    # =========================
    # 📊 FILTRO 90 DIAS
    # =========================
    corte = pd.Timestamp.today() - pd.Timedelta(days=90)
    df_90 = df_vendas[df_vendas["DATAEMISSAO"] >= corte]

    # =========================
    # 📈 AGREGAÇÃO
    # =========================
    vendas_agg = df_90.groupby("CODPRD").agg({
        "QUANTIDADE": "sum",
        "VALOR_TOTAL": "sum"
    }).reset_index()

    vendas_agg.rename(columns={
        "QUANTIDADE": "QTDE_90D",
        "VALOR_TOTAL": "FATURAMENTO_90D"
    }, inplace=True)

    vendas_agg["MEDIA_DIA"] = vendas_agg["QTDE_90D"] / 90

    # =========================
    # 🔗 JOIN COM ESTOQUE
    # =========================
    df = vendas_agg.merge(df_estoque, on="CODPRD", how="left")

    # =========================
    # 📉 COBERTURA
    # =========================
    df["COBERTURA_DIAS"] = df["ESTOQUE"] / df["MEDIA_DIA"]
    df["COBERTURA_DIAS"] = df["COBERTURA_DIAS"].replace([float("inf"), -float("inf")], 0)

    # =========================
    # 🛒 SUGESTÃO DE COMPRA
    # =========================
    DIAS_DESEJADOS = 30

    df["SUGESTAO_COMPRA"] = (DIAS_DESEJADOS * df["MEDIA_DIA"]) - df["ESTOQUE"]
    df["SUGESTAO_COMPRA"] = df["SUGESTAO_COMPRA"].apply(lambda x: max(x, 0))

    # =========================
    # 🚨 CLASSIFICAÇÃO
    # =========================
    def classificar(row):
        if row["COBERTURA_DIAS"] <= 7:
            return "URGENTE"
        elif row["COBERTURA_DIAS"] <= 15:
            return "ATENÇÃO"
        else:
            return "OK"

    df["STATUS_ESTOQUE"] = df.apply(classificar, axis=1)

    return df





def atualizar_inteligencia_produtos():
    app_log.info("Atualizando produtos_inteligencia...")

    df = gerar_inteligencia_produtos()

    if df.empty:
        app_log.info("Sem dados para inteligencia.")
        return

    client = autenticar_sheets()
    sh = client.open_by_key(PLANILHA_ID)

    try:
        aba = sh.worksheet("produtos_inteligencia")
    except Exception:
        aba = sh.add_worksheet(title="produtos_inteligencia", rows=2000, cols=20)

    set_with_dataframe(
        aba,
        df,
        row=1,
        col=1,
        include_index=False,
        resize=True
    )

    app_log.info(f"Inteligencia atualizada: {len(df)} registros.")





def atualizar_alerta_produtos():
    app_log.info("Atualizando produtos_alerta_compra...")

    df = gerar_inteligencia_produtos()

    if df.empty:
        return

    df_alerta = df[df["STATUS_ESTOQUE"].isin(["URGENTE", "ATENCAO"])]

    client = autenticar_sheets()
    sh = client.open_by_key(PLANILHA_ID)

    try:
        aba = sh.worksheet("produtos_alerta_compra")
    except Exception:
        aba = sh.add_worksheet(title="produtos_alerta_compra", rows=1000, cols=20)

    set_with_dataframe(
        aba,
        df_alerta,
        row=1,
        col=1,
        include_index=False,
        resize=True
    )

    app_log.info(f"Alerta atualizado: {len(df_alerta)} produtos críticos.")



# =======================================================================
# --- 5. EXECUÇÃO PRINCIPAL ---
# =======================================================================
def main():
    """
    Função principal que coordena o fluxo de extração e salvamento de dados.
    Mede o tempo de início e fim da automação.
    """
    app_log.info("=== INICIANDO ROTEIRO DE AUTOMAÇÃO ===")
    start_time = time.time()
    
    # 1. Extrai APENAS os dados da janela de atualização
    df_vendas_novas = extrair_dados_firebird()
    
    if not df_vendas_novas.empty:
        
        # A. Configuração de Localidade e Formatação (Aplicada apenas aos NOVOS dados)
        try: 
            # 1. PASSO CRÍTICO: Converte a coluna para o tipo datetime do Pandas (necessário para usar .dt)
            df_vendas_novas['DATAEMISSAO'] = pd.to_datetime(df_vendas_novas['DATAEMISSAO'], errors='coerce')

            # 2. Garante que DATAEMISSAO seja uma string no formato DD/MM/YYYY para o Sheets
            df_vendas_novas['DATAEMISSAO'] = df_vendas_novas['DATAEMISSAO'].dt.strftime('%d/%m/%Y') 
                
            # 3. Formata MESANO para a chave de mesclagem (MM/AAAA)
            # Reutiliza a string DD/MM/YYYY para criar o MESANO corretamente
            df_vendas_novas['MESANO'] = pd.to_datetime(df_vendas_novas['DATAEMISSAO'], format='%d/%m/%Y').dt.strftime('%m/%Y')
            
            # 4. Configura o locale para formatação de moeda
            locale.setlocale(locale.LC_ALL, 'pt_BR.utf8')
        except locale.Error:
            locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil.1252')
            
        # 3. Formatação de Moeda e Porcentagem
        colunas_moeda = ['VALORBRUTO', 'VALORLIQUIDO', 'VALORDESC', 'VALORDESCITENS']
        for col in colunas_moeda:
            # Converte para numérico antes de formatar
            df_vendas_novas[col] = pd.to_numeric(df_vendas_novas[col], errors='coerce').apply(
                lambda x: locale.format_string('%.2f', x, grouping=True) if pd.notna(x) else '')
            
        df_vendas_novas['PERCENTUALDESC'] = pd.to_numeric(df_vendas_novas['PERCENTUALDESC'], errors='coerce').apply(
            lambda x: f'{x:.2f}%' if pd.notna(x) else '')
        
    # --- LÓGICA DE HISTÓRICO E CONCATENAÇÃO ---
    
    # 1. Lê o histórico completo do Google Sheets
    df_historico = ler_historico_do_sheets() 
    
    # 2. Define quais MESES estão na Janela de Atualização (aqueles que vieram do Firebird)
    if not df_vendas_novas.empty:
        meses_para_atualizar = df_vendas_novas['MESANO'].unique()
    else:
        # Se não há dados novos, não há meses para atualizar, apenas reescreve o histórico
        meses_para_atualizar = []

    # 3. Filtra o histórico, mantendo apenas os meses que NÃO ESTÃO na janela.
    # Esta é a lógica mais robusta, baseada na string 'MM/YYYY'.
    if not df_historico.empty:
         df_historico_fechado = df_historico[~df_historico['MESANO'].isin(meses_para_atualizar)].copy()
    else:
         df_historico_fechado = pd.DataFrame()
    
    # 4. Concatena (Histórico Fechado + Dados Novos)
    if df_historico_fechado.empty:
        df_final = df_vendas_novas
    elif df_vendas_novas.empty:
        df_final = df_historico_fechado
    else:
        # Concatena os dados antigos (filtrados) com os dados novos
        df_final = pd.concat([df_historico_fechado, df_vendas_novas], ignore_index=True)

    # 5. Sobrescreve a Planilha de Vendas com o DataFrame FINAL
    sobrescrever_a_planilha(df_final)
    
    # 6. Atualiza a lista de vendedores ativo (NOVO RECURSO)
    atualizar_lista_vendedores()
    
    # 7. Atualiza a aba Clientes_Historico
    atualizar_clientes_historico()
    
    # 8. Atualiza a aba KPIs_Ciclo_Clientes
    #atualizar_kpis_ciclo_clientes()
    
    # 9. Atualiza a aba Financeiro_Titulos
    atualizar_financeiro_titulos()
    
    # 10. Vendas produtos
    atualizar_vendas_produtos()

    # 11. Estoque produtos
    atualizar_estoque_produtos()
    
    # 12. Inteligência de produtos
    atualizar_inteligencia_produtos()

    # 13. Alerta de compra
    atualizar_alerta_produtos()    
    end_time = time.time()
    tempo_total = end_time - start_time
    app_log.info(f"=== AUTOMAÇÃO FINALIZADA COM SUCESSO EM {tempo_total:.2f} SEGUNDOS ===\n")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        app_log.error(f"Falha gravíssima na execução: {e}", exc_info=True)
