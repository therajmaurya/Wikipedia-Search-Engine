from django import forms
from .models import User
class mylogin(forms.ModelForm):
    password = forms.CharField(widget=forms.PasswordInput)  # widget hidden field
    class Meta:

        model = User #we tell Django which model should be used to create this form (model = Post).
        widget = {
            'password':forms.PasswordInput(),
        }
        fields = ['username','password']