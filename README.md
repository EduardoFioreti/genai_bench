[README.md](https://github.com/user-attachments/files/34661147/README.md)
# genai_bench

Código e dados do experimento do artigo **"Integração de Inteligência Artificial Generativa em aplicações web assíncronas com Python e Django"** (Fioreti e Lima, 2026).

O projeto compara duas arquiteturas de uma aplicação Django que chama um modelo de linguagem:

| | Síncrona | Assíncrona |
|---|---|---|
| Endpoint | `POST /sync/` | `POST /async/` (responde 202) |
| Servidor | Django WSGI + Waitress (4 threads) | Django ASGI + Daphne + Channels |
| Modelo | chamado dentro da requisição | chamado por worker Celery (4 threads), fila no Redis |
| Entrega do texto | na resposta HTTP | WebSocket `ws/jobs/<job_id>/` |

**O modelo de linguagem é simulado** (padrão: latência sorteada de uma normal com média de 3 s e desvio-padrão de 1 s, definida pelo conteúdo do prompt e pela semente 42, igual nas duas arquiteturas). Também é possível usar uma API real compatível com OpenAI (`LLM_BACKEND=openai`).

## Conteúdo

- `config/`, `gen/`, `manage.py`: projeto Django (views, tarefa Celery, consumidor WebSocket, chamada ao modelo).
- `benchmark_carga.py`: gerador de carga. Mede tempo de resposta HTTP, tempo até o texto completo, taxa de sucesso e memória (RSS) dos processos.
- `prompts.txt`: 20 prompts sintéticos usados nos testes.
- `resultados/`: dados da rodada do artigo (executada em 10/11/2026), com `metadados.json`, `resumo.csv` e `brutos.csv` de cada arquitetura.
- `requirements.txt`: dependências.

## Como reproduzir

Testado com Python 3.11 no Windows. O Redis precisa estar rodando na porta 6379 (por exemplo, `docker run -d --name redis-bench -p 6379:6379 redis:7`).

```
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Em três terminais, com o ambiente ativado:

```
python -m waitress --port=8000 --threads=4 config.wsgi:application
python -m daphne -p 8001 config.asgi:application
celery -A config worker -l warning -P threads -c 4
```

Em um quarto terminal (troque os PIDs pelos dos processos reais):

```
python benchmark_carga.py --arquitetura sincrona --url http://127.0.0.1:8000/sync/ --pid web=PID_WAITRESS --niveis 5,10,20,40 --repeticoes 5 --prompts-arquivo prompts.txt --pausa 40
python benchmark_carga.py --arquitetura assincrona --url http://127.0.0.1:8001/async/ --ws-base ws://127.0.0.1:8001/ws/jobs/ --pid web=PID_DAPHNE --pid celery=PID_CELERY --niveis 5,10,20,40 --repeticoes 5 --prompts-arquivo prompts.txt --pausa 40
```

## Definições

- Falha: resposta que não chega em 30 s ou erro HTTP.
- Sucesso HTTP: resposta 2xx. Na assíncrona é a aceitação (202), e não o texto.
- Sucesso ponta a ponta: o texto chegou ao cliente (síncrona: na resposta; assíncrona: por WebSocket).
- Nível: N requisições disparadas ao mesmo tempo (rajada).
- Memória: RSS dos processos indicados em `--pid`, amostrada a cada 0,5 s (pico e média). A memória do Redis (Docker) não foi medida.

## Limitações

Modelo simulado, uma única máquina (que também executou o gerador de carga), servidores de aplicação diferentes entre as arquiteturas, execução em sequência sem aleatorização e níveis de até 40 requisições simultâneas. Detalhes no artigo.
