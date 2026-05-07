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
          echo "Arquivos encontrados:"
          find . -maxdepth 6 -type f | sort

      - name: Configurar Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Instalar dependências
        run: |
          python -m pip install --upgrade pip
          pip install -r cotacoes/requirements.txt

      - name: Rodar coletor de cotações
        run: |
          echo "Verificando caminho do coletor..."

          if [ -f "cotacoes/scripts/coletor_cotacoes_nordeste.py" ]; then
            echo "Coletor encontrado no caminho correto."
            python cotacoes/scripts/coletor_cotacoes_nordeste.py
          else
            echo "Coletor não encontrado no caminho esperado."
            echo "Arquivos Python encontrados:"
            find . -name "*.py" -type f | sort
            exit 1
          fi

      - name: Verificar arquivos gerados
        run: |
          echo "Verificando arquivos gerados..."
          find . -maxdepth 6 -type f | sort

          python -m json.tool cotacoes/public/cotacoes_nordeste.json > /dev/null

      - name: Salvar atualização no repositório
        run: |
          git config user.name "github-actions"
          git config user.email "github-actions@github.com"

          git add cotacoes/public/cotacoes_nordeste.json
          git add cotacoes/public/cotacoes_nordeste.csv
          git add cotacoes/logs/status_ultima_execucao.json

          git commit -m "Atualizar cotações Nordeste Agro" || echo "Sem alterações para commit"
          git push
