from django.urls import path

from . import views

urlpatterns = [
    path('<str:name>/_search/', views.index, name='index'),
    path('<str:name>/<str:id>', views.detail, name='detail'),
    path('fire_api/<str:protocol_type>/<str:id>', views.fire_api,
         name='fire_api')
]
