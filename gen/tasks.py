from asgiref.sync import async_to_sync
from celery import shared_task
from channels.layers import get_channel_layer

from . import llm


@shared_task(ignore_result=True)
def gerar_task(job_id: str, prompt: str):
    try:
        payload = {"status": "concluido", "job_id": job_id, **llm.gerar_texto(prompt)}
    except Exception as exc:  # falha do provedor etc.
        payload = {"status": "erro", "job_id": job_id, "erro": type(exc).__name__}
    layer = get_channel_layer()
    async_to_sync(layer.group_send)(
        f"job_{job_id}", {"type": "job.resultado", "payload": payload}
    )
