#!/usr/bin/python3
# -*- coding: utf-8 -*-
#Modules externes
import os
import re
import logging
import csv
import xml.etree.ElementTree as ET

#Modules maison 
from Alma_Apis_Interface import Alma_Apis_Records
from logs import logs

SERVICE = "Alma_Fusio_Notices_Docelec"

LOGS_LEVEL = 'INFO'
LOGS_DIR = os.getenv('LOGS_PATH')

REGION = 'EU'
API_KEY = os.getenv('PROD_NETWORK_BIB_API')
# SET_ID = '1125272040004675'

file_name = '/media/sf_LouxBox/Rapport de détection des doublons-4utf8.txt'
# file_name = "Echantillon.csv"
#On initialise le logger
logs.init_logs(LOGS_DIR,SERVICE,LOGS_LEVEL)
log_module = logging.getLogger(SERVICE)
log_module.info("Début du traitement")
nb_group = 0
num_fichier = 1
liste_des_cents = []
liste_des_rejetés = []
columns_title = ['Group Number','MMSID','Identifier','Records In Group','Operation','Material Type','Brief Level','Resource Type','Held By','Title']
liste_des_cents.append(columns_title)
liste_des_rejetés.append(columns_title)

def file_in_array(file) :
    """ Récupère le fichier csv et retourne une liste 

    Args:
        file (_string_): chemin d'accès au fichier

    Returns:
        booléen : statut de l'opération True = réussie, False = Echec
        array : liste des lignes du fichier en array
    """
    try : 
            with open(file_name, 'r', encoding='utf-8', newline='') as f:
                reader = csv.reader(f, delimiter=',')
                headers = next(reader)
                return True, list(reader)
    except Exception as e:
        log_module.error("{}::Impossible de traiter le fichier::{}".format(file_name,str(e)))
        return False, []

# On récupère le fichier
statut, rows_list = file_in_array(file_name)
if not statut :
    log_module.error("ARRET DU TRAITEMENT")
    exit(1)
log_module.info("Fichier traité")

#On va regrouper nos lignes en fonction du groupe de traitement
# On créé un dictionnaire avec une clef par numéro de groupe et une lsite vide pour chqie entrée
pivot = {i[0]: [] for i in rows_list}
for row in rows_list:
    #On ajoute nos lignes dans chaque entrée correspondante
    pivot[row[0]].append(row)
log_module.info("Regroupent des lignes par numéro de groupe : OK")
# log_module.debug(pivot)

# Traitement des cas 
log_module.info("Débu de l'analyse des données")
for key, rows in pivot.items() :
    log_module.info("Traitement du groupe {}".format(key))
    nb_pref = 0
    log_module.debug(key)
    log_module.debug(rows)
    # On détermine s'il y a bien une notice préférée
    for row in rows :
        log_module.debug(row[4])
        if row[4] == 'preferred' :
            nb_pref += 1
    # Si c'est le cas on l'envoi dans le fichier pour le traitement
    log_module.debug(nb_pref)
    if nb_pref == 1 :
        liste_des_cents.extend(rows)
        nb_group +=1
    # Sinom on met de côté pour analyse
    else :
        liste_des_rejetés.extend(rows)
    # Si j'ai traité 100 groupes, je créé un fichier avec mes candidats à la fusion. Le traitement ne traite que des lots de 100 groupes
    if nb_group%100 == 0 :
        with open('/media/sf_LouxBox/Notices_a_fusionner/notices_a_fusionner_{}.csv'.format(num_fichier), 'w') as f:
            mywriter = csv.writer(f, delimiter=',')
            mywriter.writerows(liste_des_cents)
            liste_des_cents = []
            liste_des_cents.append(columns_title)
            num_fichier +=1

#  On envoie les dernières notices dans un fichier
with open('/media/sf_LouxBox/Notices_a_fusionner/notices_a_fusionner_{}.csv'.format(num_fichier), 'w') as f:
    mywriter = csv.writer(f, delimiter=',')
    mywriter.writerows(liste_des_cents)

# On envoie nos notices à analyser dans un fichier
with open('/media/sf_LouxBox/Notices_a_fusionner/notices_a_analyser.csv', 'w') as f:
    mywriter = csv.writer(f, delimiter=',')
    mywriter.writerows(liste_des_rejetés)
    liste_des_cents = []
    liste_des_cents.append(columns_title)

log_module.info("FIN DU TRAITEMENT")
