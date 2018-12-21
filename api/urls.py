from django.urls import path

from . import views

urlpatterns = [
    path('protocol_samples/_search/', views.get_samples_protocols, name='samples_protocol'),
    path('protocol_samples/<str:id>/', views.get_samples_protocol_details, name='samples_protocol_detail'),
    path('protocol_files/_search/', views.get_files_protocols, name='files_protocol'),
    path('<str:name>/_search/', views.index, name='index'),
    path('<str:name>/<str:id>', views.detail, name='detail')
]