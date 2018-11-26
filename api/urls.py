from django.urls import path

from . import views

urlpatterns = [
    path('protocol/_search/', views.get_protocols, name='protocol'),
    path('<str:name>/_search/', views.index, name='index'),
    path('<str:name>/<str:id>', views.detail, name='detail')
]