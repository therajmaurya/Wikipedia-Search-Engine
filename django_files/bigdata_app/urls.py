from django.conf.urls import url
from .views import bigdatajob, login, logout

urlpatterns = [
    url(r'^bigdatajob/?$', bigdatajob, name='bigdatajob'),
    url(r'^login/?$', login, name='login'),
    url(r'^logout/?$', logout, name='logout'),
]