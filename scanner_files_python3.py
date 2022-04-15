# -*- coding: utf-8 -*-
"""
# ---------------------------------------------------------------------------
# Name : script_listing_files.py (python 3.x)
# Creation: 13/04/2022
# Modification :13/04/2022
# Auteur(s) : Jéros VIGAN
# Projet :
# Description : Ce script permet de lister tous les projets mxd (ou autres type de fichiers) avec leurs tailles respectives.
# ---------------------------------------------------------------------------
"""
### ===============================================================
### Importation des packages
### ===============================================================
import os,sys,time
from datetime import datetime

### ===============================================================
### Fonctions
### ===============================================================

#Lister l'aborescence d'un répertoire grâce à la fonction os.walk(path)
def recup_path(path): 
    fichier=[] 
    for root, dirs, files in os.walk(path): 
        for i in files: 
            fichier.append(os.path.join(root, i)) 
    return fichier

#Lister l'aborescence d'un répertoire grâce à la fonction glob.glob(path)
def recup_path_v2(path):
    import glob
    fichier=[] 
    l = glob.glob(path+'\\*') 
    for i in l: 
        if os.path.isdir(i): fichier.extend(listdirectory(i)) 
        else: fichier.append(i) 
    return fichier

#Lister l'aborescence d'un répertoire grâce au generateur "yield"
def recup_path_v3(path):
    '''Generateur en memoire qui permet de recuperer les chemins absolus'''
    for filename in os.listdir(path):
        localpath = os.path.join(path, filename)
        if os.path.isfile(localpath):
            yield localpath
        elif os.path.isdir(localpath):
            os.chdir(localpath)
            yield from recup_path(localpath)
            os.chdir("..")
            
#Calcul de la taille (Bytes)
def get_size(start_path):
    total_size = 0
    if not os.path.isfile(start_path):
        for dirpath, dirnames, filenames in os.walk(start_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
    else:
        total_size = os.path.getsize(start_path)
    return total_size

#Conversion des bytes en KB or MB or GB
def convert_unit(size_in_bytes, unit):
   if unit =='KB':
       return size_in_bytes/1024  #1 KilloByte == 1024 Bytes
   elif unit =='MB':
       return size_in_bytes/(1024*1024) #1 Megabyte == 1024*1024 Bytes
   elif unit =='GB':
       return size_in_bytes/(1024*1024*1024) #1 GigaByte == 1024*1024*1024 Bytes
   else:
       return size_in_bytes

#Scanner de repertoire
def scanner_rep(path,list_extension):
    dic_rep={}
    list_fitre=[]
    if os.path.exists(path):
        list_paths=recup_path(path)
        print_text('\nLe repertoire '+path+'  compte : '+str(len(list_paths))+' fichiers(peu importe l\'extension)\n')
        for file in list_paths:
            for extension in list_extension:
                file_name, file_extension = os.path.splitext(file)
                if file_extension.lower()==extension:
                    list_fitre.append(file)
        print_text('Le repertoire '+path+'  compte : '+str(len(list_fitre))+' fichiers( l\'extension :'+str(' , '.join(list_extension))+' )\n')
        time.sleep(1)
        return list_fitre
    else:
        print_text('Vérifier votre repertoire! Merci\n')

#factorisation message
def print_text(text):
    print(text)
    flog.write(text)

if __name__ == '__main__':

    #Déclaration du dossier de travail
    os.chdir(os.path.abspath(os.path.dirname(os.path.realpath("__file__"))))

    #Debut du programme
    debut = datetime.now()
    file_date=debut.strftime("%Y%m%d")
    
    #workspace
    base=os.path.abspath(os.path.dirname(os.path.realpath("__file__")))

    #Log
    if not os.path.exists(os.path.join(base+"/_log")):
        os.makedirs(os.path.join(base+"/_log"))  
    flog = open(base+"\_log\log_"+str(file_date)+'.log', "w")
    flog.write("==========\n")
    flog.write(str(datetime.now()) + "\n")
    flog.write("Début du logging! \n")

    print_text('Votre dossier de travail : '+str(os.getcwd())+'\n')
    time.sleep(0.2)

    #Ecriture dans un fichier csv
    if not os.path.exists(os.path.join(base+"/_listes")):
        os.makedirs(os.path.join(base+"/_listes"))
    myText = open(base+"\_listes\listes_"+str(file_date)+'.csv', 'w')
    myText.write('taille_fichier;taille_convertie;chemin_fichier\n')

    #Paramètres   
    path=input('Repertoire à scanner : ')
    if path=='':
        if 'HOME' in os.environ:
            path=os.environ['HOME']
        else:
          path='C:\\'

    ext=input('Extension à scanner ( example : .mxd,.txt ) : ')
    if ext=='':
        list_extension=['.mxd']
    else:
        list_extension=list(ext.split(','))

    unit=input('Unité de calcul : KB or MB or GB : ')
    if unit=='':
        unit='MB'

    #Scanner les fichiers
    list_files=scanner_rep(path,list_extension)
    l=len(list_files)

    if l!=0:
        print('Le repertoire '+path+'  compte : '+str(l)+'\n')
        flog.write('Le repertoire '+path+'  compte : '+str(l)+'\n')
        time.sleep(1)
        for cpt,_file in enumerate(list_files):
            taille=get_size(_file)
            taille_convert=convert_unit(taille, unit)
            print('\n----------------------------------------------')
            print_text('\n--- '+str(cpt+1)+'/'+str(l))
            print_text('\n- taille = '+str(taille))
            print_text('\n- taille en '+unit+' = '+str(taille_convert))
            print_text('\n'+str(_file))
            flog.write('\n\n')
            myText.write(str(taille)+';'+str(taille_convert)+';'+str(_file)+"\n")
            time.sleep(0.1)
    else:
        print_text('Le repertoire '+path+'  compte : '+str(0)+'\n')
        myText.close()

    #CSV
    myText.close()

    #LOG
    print_text("\nFin traitement \n")
    print_text('la durée de traitement est ' + str(datetime.now()-debut) + 's \n')
    flog.close()