from django.urls import path
from . import views

app_name = "core"

urlpatterns = [
    path('publish/', views.PublishAPI.as_view()),
    path('subscribe/', views.SubscribeAPI.as_view()),
]
