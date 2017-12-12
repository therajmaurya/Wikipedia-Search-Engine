from django import forms
from .models import User

class myhome(forms.Form):
    text = forms.CharField(widget=forms.Textarea)
    class Meta:
        fields=["text"]