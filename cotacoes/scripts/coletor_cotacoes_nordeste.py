name: Atualizar Cotações Nordeste Agro

on:
  schedule:
    # 11:00 UTC = 08:00 no horário de Fortaleza
    - cron: "0 11 * * *"

  workflow_dispatch:

permissions:
  contents: write

jobs:
  atualizar-cotacoes:
    runs-on: ubuntu-latest

    steps:
      - name: Baixar repositório
        uses: actions/checkout@v4

      - name: Mostrar estrutura do repositório
        run: |
          echo "Pasta atual:"
          pwd
          echo ""
          echo "Arquivos encontrados no repositório:"
          find . -maxdepth 8 -type f | sort

      - name: Configurar Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Instalar dependências
        run: |
          python -m pip install --upgrade pip

          if [ -f "cotacoes/requirements.txt" ]; then
            echo "Instalando dependências de cotacoes/requirements.txt"
            pip install -r cotacoes/requirements.txt
          elif [ -f "requirements.txt" ]; then
            echo "Instalando dependências de requirements.txt"
            pip install -r requirements.txt
          else
            echo "requirements.txt não encontrado. Instalando dependências padrão."
            pip install requests==2.32.3 beautifulsoup4==4.12.3
          fi

      - name: Localizar coletor de cotações
        id: localizar-coletor
        run: |
          echo "Procurando coletor..."

          COLETOR_PATH=""

          if [ -f "cotacoes/scripts/coletor_cotacoes_nordeste.py" ]; then
            COLETOR_PATH="cotacoes/scripts/coletor_cotacoes_nordeste.py"
          elif [ -f "cotacoes/scripts/coletor_cotacoes_nordeste_v111.py" ]; then
            COLETOR_PATH="cotacoes/scripts/coletor_cotacoes_nordeste_v111.py"
          else
            COLETOR_PATH=$(find . -type f \( -name "coletor_cotacoes_nordeste.py" -o -name "coletor_cotacoes_nordeste_v111.py" \) | head -n 1)
          fi

          if [ -z "$COLETOR_PATH" ]; then
            echo "ERRO: coletor não encontrado."
            echo ""
            echo "O arquivo precisa estar em um destes caminhos:"
            echo "cotacoes/scripts/coletor_cotacoes_nordeste.py"
            echo "ou"
            echo "cotacoes/scripts/coletor_cotacoes_nordeste_v111.py"
            echo ""
            echo "Arquivos Python encontrados:"
            find . -type f -name "*.py" | sort
            exit 1
          fi

          echo "Coletor encontrado em: $COLETOR_PATH"
          echo "COLETOR_PATH=$COLETOR_PATH" >> "$GITHUB_ENV"

      - name: Rodar coletor de cotações
        run: |
          echo "Rodando coletor:"
          echo "$COLETOR_PATH"
          python "$COLETOR_PATH"

      - name: Verificar arquivos gerados
        run: |
          echo "Verificando arquivos gerados..."

          echo ""
          echo "Arquivos JSON encontrados:"
          find . -type f -name "*.json" | sort

          echo ""
          echo "Arquivos CSV encontrados:"
          find . -type f -name "*.csv" | sort

          if [ -f "cotacoes/public/cotacoes_nordeste.json" ]; then
            python -m json.tool cotacoes/public/cotacoes_nordeste.json > /dev/null
            echo "cotacoes/public/cotacoes_nordeste.json OK"
          else
            echo "Aviso: cotacoes/public/cotacoes_nordeste.json não encontrado."
          fi

          if [ -f "cotacoes/public/cotacoes_regionais.json" ]; then
            python -m json.tool cotacoes/public/cotacoes_regionais.json > /dev/null
            echo "cotacoes/public/cotacoes_regionais.json OK"
          else
            echo "Aviso: cotacoes/public/cotacoes_regionais.json não encontrado."
          fi

      - name: Salvar atualização no repositório
        run: |
          git config user.name "github-actions"
          git config user.email "github-actions@github.com"

          git add cotacoes/public/*.json || true
          git add cotacoes/public/*.csv || true
          git add cotacoes/logs/*.json || true

          git commit -m "Atualizar cotações Nordeste Agro" || echo "Sem alterações para commit"
          git push
