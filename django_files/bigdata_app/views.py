from django.shortcuts import render
from django.http import HttpResponse, HttpResponseRedirect
from django.contrib.auth.decorators import login_required
from django.contrib.auth import authenticate, login as auth_login, logout as auth_logout
from .login import mylogin
from .home import myhome
import os
import subprocess
import requests, json, pprint, textwrap



def login(request):
    form1 = mylogin()
    logout(request)
    if request.POST:
        uid = request.POST['username']
        passwd = request.POST['password']
        user = authenticate(username=uid, password=passwd)
        if user is not None:
            auth_login(request, user)
            return HttpResponseRedirect("bigdatajob")
        else:
            return HttpResponse("Your username or password didn't match.")
    return render(request, "login.html", {"form": form1})


@login_required(login_url='login')
def bigdatajob(request):
    form2 = myhome(request.POST or None)
    if request.POST:
        if form2.is_valid():
            text = form2.cleaned_data["text"]
            #os.system("e:")
            #os.system(r"cd \files\apache_spark")
            #cmd = subprocess.Popen(["spark-submit", "TF-IDF.py", text], shell=True, stdin=subprocess.PIPE,
            #                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            cmd = subprocess.Popen(["spark-submit", "TF-IDF.py", text], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, stdin=subprocess.PIPE)
            output = cmd.communicate()[0]
            #output = "Testing Script Run!!"

            return HttpResponse(output)
    return render(request, "bigdata_app/home.html", {"form": form2})


def logout(request):
    auth_logout(request)
    return HttpResponseRedirect("login")