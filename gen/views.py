import json
import uuid

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from . import llm
from .tasks import gerar_task


def _ler(request):
    try:
        return json.loads(request.body or b"{}")
    except json.JSONDecodeError:
        return {}


def saude(request):
    return JsonResponse({"status": "ok"})


@csrf_exempt
@require_POST
def gerar_sync(request):
    """ARQUITETURA SÍNCRONA: chama o modelo dentro da própria requisição."""
    prompt = _ler(request).get("prompt", "")
    resultado = llm.gerar_texto(prompt)
    return JsonResponse({"status": "concluido", **resultado}, status=200)


@csrf_exempt
@require_POST
def gerar_async(request):
    """ARQUITETURA ASSÍNCRONA: enfileira a tarefa e responde 202 na hora.
    O cliente abre o WebSocket ws/jobs/<job_id>/ ANTES de enviar este POST."""
    dados = _ler(request)
    job_id = dados.get("job_id") or uuid.uuid4().hex
    gerar_task.delay(job_id, dados.get("prompt", ""))
    return JsonResponse({"status": "aceito", "job_id": job_id}, status=202)
