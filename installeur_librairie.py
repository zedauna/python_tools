# -*- coding: utf-8 -*-
"""
# ---------------------------------------------------------------------------
# Version: python 3.x
# Nom :installeur_libs.py
# Creation :02/04/2022
# Modification :02/04/2022
# Auteur(s) : Jéros VIGAN 
# Projet : 
# Description :Cela permet de calculer le delta des libraires installées, 
#                puis d'installer les librairies necessaires
# ---------------------------------------------------------------------------
"""
import subprocess,sys,pkg_resources

def install_libs(required):
    '''Cette fonction permet de calculer le delta et d'installer les libriares necessaires'''
    installed = {pkg.key for pkg in pkg_resources.working_set}
    missing = required - installed
    
    if missing:
        for i in missing:
            print("\n-----------Installation de la librairie : "+i+" sur votre ordinateur---------------\n")
        python = sys.executable
        subprocess.check_call([python, '-m', 'pip', 'install', *missing])
        print("\n-------------Fin des installations des libraires necessaires !----------------\n")
    
required = {'pandas', 'numpy'}
install_libs(required)