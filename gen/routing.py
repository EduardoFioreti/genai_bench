from django.urls import re_path

from .consumers import JobConsumer

websocket_urlpatterns = [
    re_path(r"^ws/jobs/(?P<job_id>[0-9a-zA-Z]+)/$", JobConsumer.as_asgi()),
]
