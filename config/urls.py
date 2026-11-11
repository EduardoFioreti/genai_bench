from django.urls import path
from gen import views

urlpatterns = [
    path("saude/", views.saude),
    path("sync/", views.gerar_sync),
    path("async/", views.gerar_async),
]
