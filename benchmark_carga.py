#!/usr/bin/env python3
"""
Teste de carga para comparar a arquitetura SÍNCRONA e a ASSÍNCRONA.

Instalação:   pip install httpx psutil
Uso (exemplo, com o projeto genai_bench):
  python benchmark_carga.py --arquitetura sincrona \
      --url http://127.0.0.1:8000/sync/ \
      --pid web=PID_DO_WAITRESS \
      --niveis 5,10,25,50,100 --repeticoes 5 --prompts-arquivo prompts.txt

  python benchmark_carga.py --arquitetura assincrona \
      --url http://127.0.0.1:8001/async/ --ws-base ws://127.0.0.1:8001/ws/jobs/ \
      --pid web=PID_DO_DAPHNE --pid celery=PID_DO_WORKER --pid redis=PID_DO_REDIS \
      --niveis 5,10,25,50,100 --repeticoes 5 --prompts-arquivo prompts.txt

Rode as DUAS arquiteturas com os MESMOS níveis, repetições e prompts.
Cada execução cria uma pasta em resultados/ com:
  - metadados.json  -> computador, sistema, versões (use na seção 3.2)
  - brutos.csv      -> uma linha por requisição
  - resumo.csv      -> uma linha por nível e repetição

Definições usadas (declare no artigo exatamente assim, ou altere o script):
  - Requisição com sucesso (HTTP): resposta 2xx dentro do tempo limite.
    Na arquitetura assíncrona isso é a ACEITAÇÃO (202), e não a entrega do texto.
  - Sucesso ponta a ponta (e2e): o texto chegou ao cliente dentro do tempo limite
    (síncrona: a própria resposta; assíncrona: mensagem recebida por WebSocket).
  - Tempo de resposta HTTP: do envio do POST até a resposta HTTP.
  - Tempo até o resultado: do envio do POST até o texto completo chegar.
  - Nível de concorrência: N requisições disparadas ao mesmo tempo (rajada).
  - Memória: RSS (MB) dos processos indicados em --pid, amostrada a cada 0,5 s;
    guarda-se o pico e a média durante cada rajada.

ATENÇÃO: cada requisição chama a API real do modelo (custo e limite de taxa).
Comece com níveis pequenos. Se o provedor devolver erro 429, isso contamina o
resultado: registre e comente no artigo, ou reduza a carga.
"""
import argparse
import asyncio
import csv
import datetime as dt
import json
import os
import platform
import random
import statistics
import sys
import threading
import time
import uuid
from importlib import metadata

import httpx
import psutil

try:
    import websockets
except ImportError:
    websockets = None


# ---------------------------------------------------------------- metadados
def nome_cpu():
    try:
        if sys.platform.startswith("linux"):
            with open("/proc/cpuinfo", encoding="utf8") as f:
                for linha in f:
                    if linha.lower().startswith("model name"):
                        return linha.split(":", 1)[1].strip()
        return platform.processor() or platform.machine()
    except Exception:
        return platform.machine()


def versoes(pacotes):
    out = {}
    for p in pacotes:
        try:
            out[p] = metadata.version(p)
        except metadata.PackageNotFoundError:
            out[p] = None
    return out


def coletar_metadados(args):
    return {
        "data_hora": dt.datetime.now().isoformat(timespec="seconds"),
        "arquitetura": args.arquitetura,
        "url": args.url,
        "niveis": args.niveis,
        "repeticoes": args.repeticoes,
        "aquecimento": args.aquecimento,
        "timeout_s": args.timeout,
        "ws_base": args.ws_base,
        "pausa_s": args.pausa,
        "semente": args.semente,
        "processos_monitorados": args.pid,
        "computador": {
            "sistema": f"{platform.system()} {platform.release()}",
            "versao_sistema": platform.version(),
            "processador": nome_cpu(),
            "nucleos_fisicos": psutil.cpu_count(logical=False),
            "nucleos_logicos": psutil.cpu_count(logical=True),
            "memoria_total_gb": round(psutil.virtual_memory().total / 1024**3, 1),
            "python": sys.version.split()[0],
        },
        "pacotes": versoes(
            ["django", "celery", "redis", "channels", "channels-redis", "daphne",
             "waitress", "httpx", "websockets", "psutil"]
        ),
    }


# ---------------------------------------------------------------- memória
class Memoria:
    """Amostra o RSS (MB) de cada processo nomeado (inclui filhos)."""

    def __init__(self, pids):
        self.procs = {}
        for nome, pid in pids.items():
            try:
                self.procs[nome] = psutil.Process(pid)
            except psutil.Error as e:
                sys.exit(f"Não achei o processo '{nome}' (PID {pid}): {e}")
        self.amostras = {n: [] for n in self.procs}
        self._parar = threading.Event()
        self._t = None

    def _rss(self, proc):
        total = 0
        try:
            lista = [proc] + proc.children(recursive=True)
        except psutil.Error:
            lista = [proc]
        for p in lista:
            try:
                total += p.memory_info().rss
            except psutil.Error:
                pass
        return total / 1024 / 1024

    def _loop(self):
        while not self._parar.is_set():
            for n, p in self.procs.items():
                self.amostras[n].append(self._rss(p))
            self._parar.wait(0.5)

    def iniciar(self):
        self.amostras = {n: [] for n in self.procs}
        self._parar.clear()
        self._t = threading.Thread(target=self._loop, daemon=True)
        self._t.start()

    def parar(self):
        self._parar.set()
        self._t.join()
        res = {}
        for n, a in self.amostras.items():
            res[f"mem_{n}_pico_mb"] = round(max(a), 1) if a else None
            res[f"mem_{n}_media_mb"] = round(statistics.mean(a), 1) if a else None
        return res


# ---------------------------------------------------------------- requisições
async def um_pedido(client, url, gerar_payload, timeout, ws_base=None):
    """Retorna (ok_http, status, t_http, ok_e2e, t_resultado)."""
    payload = gerar_payload()
    ws = None
    t0 = time.perf_counter()
    ok, status, t_http, ok_e2e, t_res = False, None, None, False, None
    try:
        if ws_base:
            job_id = uuid.uuid4().hex
            payload["job_id"] = job_id
            # o cliente escuta ANTES de enviar o POST (evita perder a mensagem)
            ws = await websockets.connect(f"{ws_base}{job_id}/", open_timeout=timeout)
        t0 = time.perf_counter()
        r = await client.post(url, json=payload, timeout=timeout)
        t_http = time.perf_counter() - t0
        ok = 200 <= r.status_code < 300
        status = r.status_code
        if ws is None:                      # síncrona: a resposta já é o texto
            ok_e2e, t_res = ok, t_http
        elif ok:                            # assíncrona: espera o WebSocket
            msg = await asyncio.wait_for(ws.recv(), timeout=max(0.1, timeout - t_http))
            t_res = time.perf_counter() - t0
            ok_e2e = json.loads(msg).get("status") == "concluido"
    except Exception as e:  # timeout, conexão recusada etc.
        status = status if status is not None else type(e).__name__
        if t_http is None:
            t_http = time.perf_counter() - t0
        if ws is None and ok is False:
            ok_e2e = False
    finally:
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass
    return ok, status, t_http, ok_e2e, t_res


async def rajada(client, url, gerar_payload, timeout, n, ws_base=None):
    return await asyncio.gather(
        *[um_pedido(client, url, gerar_payload, timeout, ws_base) for _ in range(n)]
    )


def percentil(valores, p):
    if not valores:
        return None
    v = sorted(valores)
    k = (len(v) - 1) * p
    f, c = int(k), min(int(k) + 1, len(v) - 1)
    return v[f] + (v[c] - v[f]) * (k - f)


# ---------------------------------------------------------------- principal
async def executar(args):
    rnd = random.Random(args.semente)
    if args.prompts_arquivo:
        with open(args.prompts_arquivo, encoding="utf8") as f:
            prompts = [l.strip() for l in f if l.strip()]
    else:
        prompts = ["Explique em um parágrafo o que é uma fila de tarefas."]

    def gerar_payload():
        return {args.campo_prompt: rnd.choice(prompts)}

    pids = {}
    for item in args.pid:
        nome, pid = item.split("=", 1)
        pids[nome] = int(pid)
    mem = Memoria(pids) if pids else None

    pasta = os.path.join(
        "resultados", f"{args.arquitetura}_{dt.datetime.now():%Y%m%d_%H%M%S}"
    )
    os.makedirs(pasta, exist_ok=True)
    with open(os.path.join(pasta, "metadados.json"), "w", encoding="utf8") as f:
        json.dump(coletar_metadados(args), f, ensure_ascii=False, indent=2)

    limites = httpx.Limits(max_connections=None, max_keepalive_connections=None)
    brutos, resumo = [], []
    async with httpx.AsyncClient(limits=limites) as client:
        if args.aquecimento:
            print(f"Aquecimento: {args.aquecimento} requisições...")
            await rajada(client, args.url, gerar_payload, args.timeout, args.aquecimento, args.ws_base)
            await asyncio.sleep(args.pausa)

        for nivel in args.niveis:
            for rep in range(1, args.repeticoes + 1):
                if mem:
                    mem.iniciar()
                t0 = time.perf_counter()
                res = await rajada(client, args.url, gerar_payload, args.timeout, nivel, args.ws_base)
                dur = time.perf_counter() - t0
                m = mem.parar() if mem else {}

                oks = [t for ok, _, t, _, _ in res if ok]
                e2e = [tr for _, _, _, oe, tr in res if oe and tr is not None]
                for i, (ok, status, t, oe, tr) in enumerate(res, 1):
                    brutos.append(
                        dict(arquitetura=args.arquitetura, nivel=nivel,
                             repeticao=rep, indice=i, ok_http=int(ok),
                             status=status, tempo_http_s=round(t, 4),
                             ok_e2e=int(oe),
                             tempo_resultado_s=round(tr, 4) if tr is not None else None)
                    )
                linha = dict(
                    arquitetura=args.arquitetura, nivel=nivel, repeticao=rep,
                    enviadas=nivel, sucesso=len(oks),
                    taxa_sucesso_pct=round(100 * len(oks) / nivel, 1),
                    tempo_medio_ok_s=round(statistics.mean(oks), 4) if oks else None,
                    tempo_p50_ok_s=round(percentil(oks, 0.5), 4) if oks else None,
                    tempo_p95_ok_s=round(percentil(oks, 0.95), 4) if oks else None,
                    sucesso_e2e=len(e2e),
                    taxa_e2e_pct=round(100 * len(e2e) / nivel, 1),
                    tempo_resultado_medio_s=round(statistics.mean(e2e), 4) if e2e else None,
                    tempo_resultado_p95_s=round(percentil(e2e, 0.95), 4) if e2e else None,
                    duracao_rajada_s=round(dur, 2), **m,
                )
                resumo.append(linha)
                print(
                    f"nível {nivel:>4} | rep {rep} | sucesso {linha['taxa_sucesso_pct']:>5}% "
                    f"| tempo HTTP {linha['tempo_medio_ok_s']} s | e2e {linha['taxa_e2e_pct']}% "
                    f"tempo até o texto {linha['tempo_resultado_medio_s']} s | {m}"
                )
                await asyncio.sleep(args.pausa)

    def salvar(nome, linhas):
        if not linhas:
            return
        with open(os.path.join(pasta, nome), "w", newline="", encoding="utf8") as f:
            w = csv.DictWriter(f, fieldnames=list(linhas[0].keys()))
            w.writeheader()
            w.writerows(linhas)

    salvar("brutos.csv", brutos)
    salvar("resumo.csv", resumo)

    # Maior nível cuja taxa de sucesso média atinge o limiar (critério declarado)
    por_nivel = {}
    for l in resumo:
        por_nivel.setdefault(l["nivel"], []).append(l["taxa_sucesso_pct"])
    aprovados = [n for n, v in por_nivel.items()
                 if statistics.mean(v) >= args.limiar_sucesso]
    print("\n=== Resultado por nível (média das repetições) ===")
    for n, v in por_nivel.items():
        print(f"nível {n:>4}: taxa de sucesso média {statistics.mean(v):.1f}%")
    print(
        f"Maior nível com taxa de sucesso >= {args.limiar_sucesso}%: "
        f"{max(aprovados) if aprovados else 'nenhum'}"
    )
    print(f"Arquivos salvos em: {pasta}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--arquitetura", required=True, choices=["sincrona", "assincrona"])
    ap.add_argument("--url", required=True, help="endpoint que recebe o POST")
    ap.add_argument("--niveis", type=lambda s: [int(x) for x in s.split(",")],
                    default=[5, 10, 25, 50, 100], help="ex.: 5,10,25,50,100")
    ap.add_argument("--repeticoes", type=int, default=5)
    ap.add_argument("--aquecimento", type=int, default=3,
                    help="requisições de aquecimento antes de medir")
    ap.add_argument("--timeout", type=float, default=30.0,
                    help="segundos; acima disso a requisição conta como falha")
    ap.add_argument("--ws-base", help="ex.: ws://127.0.0.1:8001/ws/jobs/ (só na assíncrona)")
    ap.add_argument("--pausa", type=float, default=5.0,
                    help="segundos de descanso entre rajadas")
    ap.add_argument("--pid", action="append", default=[],
                    help="NOME=PID de um processo a monitorar (repetível). "
                         "Ex.: --pid web=1234 --pid celery=5678")
    ap.add_argument("--prompts-arquivo", help="arquivo .txt, um prompt por linha")
    ap.add_argument("--campo-prompt", default="prompt",
                    help="nome do campo JSON do prompt no seu endpoint")
    ap.add_argument("--semente", type=int, default=42)
    ap.add_argument("--limiar-sucesso", type=float, default=99.0,
                    help="taxa mínima (%%) para considerar o nível 'suportado'")
    args = ap.parse_args()
    if args.ws_base and websockets is None:
        sys.exit("Instale antes: pip install websockets")
    asyncio.run(executar(args))


if __name__ == "__main__":
    main()
