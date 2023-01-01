from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from util.constant import FUNCTIONS_DICT_FOR_CHANNEL
from util.multiple_server_manager import MultipleServerManager


class SubscribeAPI(APIView):

    def get(self, request, format=None):
        MultipleServerManager().subscribe(channels_functions_dict=FUNCTIONS_DICT_FOR_CHANNEL)
        return Response("SubscribeAPI", status=status.HTTP_200_OK)


class PublishAPI(APIView):

    def post(self, request, format=None):
        server_name = request.data.get('server', "Server_B")
        data = request.data.get('data', {"test": "123"})
        MultipleServerManager().publish(server_name, data)
        return Response("PublishAPI", status=status.HTTP_201_CREATED)
