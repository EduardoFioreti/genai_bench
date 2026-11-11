"""Chamada ao modelo de linguagem (idêntica nas duas arquiteturas)."""
import random
import time
import zlib

import httpx
from django.conf import settings


def _latencia_mock(prompt: str) -> float:
    # Mesma latência para o mesmo prompt, em qualquer arquitetura.
    rnd = random.Random(settings.MOCK_SEMENTE + zlib.crc32(prompt.encode("utf8")))
    lat = rnd.gauss(settings.MOCK_LATENCIA_MEDIA_S, settings.MOCK_LATENCIA_DESVIO_S)
    return max(0.3, lat)


def gerar_texto(prompt: str) -> dict:
    if settings.LLM_BACKEND == "openai":
        r = httpx.post(
            f"{settings.LLM_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {settings.LLM_API_KEY}"},
            json={
                "model": settings.LLM_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": settings.LLM_MAX_TOKENS,
                "temperature": settings.LLM_TEMPERATURE,
            },
            timeout=settings.LLM_TIMEOUT_S,
        )
        r.raise_for_status()
        texto = r.json()["choices"][0]["message"]["content"]
        return {"texto": texto, "backend": "openai", "modelo": settings.LLM_MODEL}
    espera = _latencia_mock(prompt)
    time.sleep(espera)  # bloqueia como uma chamada de rede real bloquearia
    return {
        "texto": f"[texto simulado] resposta para: {prompt[:60]}",
        "backend": "mock",
        "latencia_simulada_s": round(espera, 3),
    }
