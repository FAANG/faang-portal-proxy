from django.urls import path

from . import views

urlpatterns = [
    path('<str:name>/_search', views.index, name='index')
]