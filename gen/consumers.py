from channels.generic.websocket import AsyncJsonWebsocketConsumer


class JobConsumer(AsyncJsonWebsocketConsumer):
    async def connect(self):
        self.group = f"job_{self.scope['url_route']['kwargs']['job_id']}"
        await self.channel_layer.group_add(self.group, self.channel_name)
        await self.accept()

    async def disconnect(self, code):
        await self.channel_layer.group_discard(self.group, self.channel_name)

    async def job_resultado(self, event):
        await self.send_json(event["payload"])
