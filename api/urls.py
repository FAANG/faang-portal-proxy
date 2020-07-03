from django.urls import path, re_path

from . import views

urlpatterns = [
    path('<str:name>/_search/', views.index, name='index'),
    path('<str:name>/<str:id>', views.detail, name='detail'),
    path('fire_api/trackhubregistry/<str:genome_id>/<str:folder>/<str:doc_id>',
         views.trackhubregistry_with_dirs_fire_api,
         name='trackhubregistry_with_dirs_fire_api'),
    path('fire_api/trackhubregistry/<str:genome_id>/<str:doc_id>',
         views.trackhubregistry_with_dir_fire_api,
         name='trackhubregistry_with_dir_fire_api'),
    path('fire_api/trackhubregistry/<str:doc_id>',
         views.trackhubregistry_fire_api, name='trackhubregistry_fire_api'),
    path('fire_api/<str:protocol_type>/<str:id>', views.protocols_fire_api,
         name='protocols_fire_api'),
    path('summary', views.summary_api, name='summary')
]
