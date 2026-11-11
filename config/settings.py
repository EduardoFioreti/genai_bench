"""Configurações mínimas do projeto de benchmark (sem banco, sem login)."""
import os

SECRET_KEY = "somente-para-benchmark-local"
DEBUG = False
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = ["channels", "gen"]
MIDDLEWARE = []  # sem middlewares, para medir só a arquitetura
ROOT_URLCONF = "config.urls"
WSGI_APPLICATION = "config.wsgi.application"
ASGI_APPLICATION = "config.asgi.application"
DATABASES = {"default": {"ENGINE": "django.db.backends.dummy"}}
USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")

CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {"hosts": [f"{REDIS_URL}/0"]},
    }
}
CELERY_BROKER_URL = f"{REDIS_URL}/1"
CELERY_TASK_IGNORE_RESULT = True  # o resultado é entregue por WebSocket
CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True

# ---- Modelo de linguagem ----
# LLM_BACKEND=mock  -> servidor simulado: espera uma latência fixa por prompt
#                      (a mesma nas duas arquiteturas). Sem custo, sem limite de taxa.
# LLM_BACKEND=openai -> API real compatível com OpenAI (precisa de chave).
LLM_BACKEND = os.environ.get("LLM_BACKEND", "mock")
MOCK_LATENCIA_MEDIA_S = float(os.environ.get("MOCK_LATENCIA_MEDIA_S", "3.0"))
MOCK_LATENCIA_DESVIO_S = float(os.environ.get("MOCK_LATENCIA_DESVIO_S", "1.0"))
MOCK_SEMENTE = int(os.environ.get("MOCK_SEMENTE", "42"))
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1")
LLM_API_KEY = os.environ.get("OPENAI_API_KEY", "")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o-mini")
LLM_MAX_TOKENS = int(os.environ.get("LLM_MAX_TOKENS", "300"))
LLM_TEMPERATURE = float(os.environ.get("LLM_TEMPERATURE", "0.7"))
LLM_TIMEOUT_S = float(os.environ.get("LLM_TIMEOUT_S", "120"))
