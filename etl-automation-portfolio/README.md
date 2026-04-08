# Python Automation Portfolio: ETL Firebird to Google Sheets

**Autor:** João Otávio Mota Roriz  
**Contato:** [LinkedIn](https://www.linkedin.com/in/joaootaviororiz/) | [GitHub](https://github.com/joaootaviororiz)

---

## 📌 Sobre o Projeto

Este projeto consiste em um script de **ETL (Extract, Transform, Load)** desenvolvido em Python com foco em automatizar o acesso e a manipulação de dados organizacionais. O script realiza a extração inteligente de relatórios financeiros e de fluxo de estoque a partir de um banco de dados relacional (Firebird), trata os dados calculando KPI's e classifica perfis de cliente através de DataFrames usando a biblioteca Pandas. Finalmente, ele exporta os resultados tratados de maneira performática para um painel gerencial no Google Sheets.

Essa solução foi desenhada pensando em **robustez**, **boas práticas de código limpo (Clean Code)** e **segurança das informações**, contendo geração de relatórios de logs da automatização, padronização via PEP8 e ocultamento de credenciais usando variáveis de ambiente.

Projeto de automação de dados que realiza um pipeline ETL completo:

- Extração de dados de banco Firebird
- Transformação e tratamento com Python
- Carga em Google Sheets para análise

---

## 📌 Funcionalidades

- Consulta automatizada ao banco de dados
- Tratamento e limpeza de dados
- Envio automático para planilhas Google
- Geração de logs de execução


### 🛠️ Tecnologias e Ferramentas Empregadas
- **Python 3+**: Linguagem core do script;
- **Pandas**: Manipulação, cálculo de KPI's e tratamento dos dados matriciais;
- **FDB**: Cliente (Driver) para conexão transparente junto ao banco do Firebird;
- **GSpread / GSpread-Dataframe**: Acesso seguro as APIs do Google Sheets API e Google Drive API;
- **Python-Dotenv**: Camada de proteção para evitar senhas expostas via leitura de variáveis de ambiente (`.env`);
- **Logging**: Configurado de forma nativa para controle de execução.

---

## 🚀 Como Configurar e Executar

Siga os passos abaixo para testar este projeto no seu ambiente.

### 1. Clonar o Repositório e Instalar Dependências

Abra seu terminal e execute:

```bash
git clone https://github.com/joaootaviororiz/sua-url-aqui.git
cd sua-url-aqui
pip install -r requirements.txt
```

### 2. Configurar Variáveis de Ambiente

Na raiz do projeto, você encontrará o arquivo `.env.example`.
Copie este arquivo, renomeie-o para `.env` e preencha com as suas próprias credenciais:

```env
# === CREDENCIAIS DO FIREBIRD ===
FIREBIRD_HOST=127.0.0.1
FIREBIRD_DB=C:\dados\seu_banco.fdb
FIREBIRD_USER=SYSDBA
FIREBIRD_PASS=suasenha
FIREBIRD_CHARSET=WIN1252

# === CREDENCIAIS DO GOOGLE SHEETS ===
GOOGLE_CREDENTIALS_FILE=sua_chave_de_servico_google_api.json
PLANILHA_ID=1bvkZNo107R...
```

*Nota: Você precisará disponibilizar sua própria chave `JSON` de serviço gerada via Console Cloud do Google para que a API Gspread possa autenticar com sua planilha.*

### 3. Uso
Execute pelo terminal:

```bash
python etl_pipeline.py
```

## 📊 Auditoria e Logs

A aplicação possui um sistema embarcado avançado de monitoramento (`logging.handlers.TimedRotatingFileHandler`).

- **Rotacionamento Diário:** Quando ativada, a aplicação não polui o console; ela gera todos os registros (erros do banco, falha em autenticação e cronometragens) e os armazena de forma organizada dentro de uma pasta nativa chamada `/logs`.
- Os arquivos são segmentados diariamente e formatados para reter apenas os últimos 7 dias de processamento.
- **Métricas de Performance:** O script audita internamente cada processamento da tabela, registrando os milissegundos dispendidos, fornecendo no final a auditoria `=== AUTOMAÇÃO FINALIZADA COM SUCESSO EM X SEGUNDOS ===`.

### 4. Execução Automática (Task Scheduler)

Este repositório contém um arquivo chamado `rodar_script.bat` já parametrizado para facilitar a execução sem depender de IDEs abertas na sua tela.
Para garantir que o robô seja executado todos os dias pontualmente pela manhã, logo que o seu computador for ativado:

1. Abra o painel **Agendador de Tarefas do Windows**.
2. Clique em **"Criar Tarefa Básica"** e defina o nome (ex: "Automação ETL Sheets").
3. Na guia *Gatilhos / Disparador*, escolha **"Ao fazer logon"** (ou "Diariamente: 08h00").
4. Na guia *Ações*, selecione **"Iniciar um Programa"** e mande procurar o caminho exato do seu arquivo `rodar_script.bat`. 

Pronto! Assim que a máquina ligar, o agendador chama o arquivo .BAT que tem a permissão para entrar no terminal submerso, rodar o ambiente Python isolado acoplado com o `.env` e realizar os uploads rotineiros do seu banco à nuvem do Google enquanto guarda todos os históricos diários cronometrados diretamente em sua pasta `/logs` para sua supervisão, zero fricções!

---

*Desenvolvido com foco em escalabilidade, eficiência e qualidade exigida por ambientes corporativos.*
