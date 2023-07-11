#!/usr/bin/python3
# -*- coding: utf-8 -*-
#Modules externes
import os
import re
import logging
import csv
import xml.etree.ElementTree as ET
import AlmaRecord

#Modules maison 
from Alma_Apis_Interface import Alma_Apis_Records
from logs import logs

SERVICE = "Alma_Fusion_Notices_Docelec"

LOGS_LEVEL = 'DEBUG'
LOGS_DIR = os.getenv('LOGS_PATH')

REGION = 'EU'
API_KEY = os.getenv('PROD_NETWORK_BIB_API')

REP = '/media/sf_LouxBox/Notices_a_fusionner/'
# SET_ID = '1125272040004675'

file_name = '/media/sf_LouxBox/Duplicate Title Analysis - 04_07_2023 13_14_13 CEST_13737001090004671.csv'
# file_name = "EchantillonTraitement.csv"
#On initialise le logger
logs.init_logs(LOGS_DIR,SERVICE,LOGS_LEVEL)
log_module = logging.getLogger(SERVICE)
log_module.info("Début du traitement")
nb_group = 0
num_fichier = 1
liste_des_cents = []
columns_title = ['Group Number','MMSID','Identifier','Records In Group','Operation','Material Type','Brief Level','Resource Type','Held By','Title']
liste_des_cents.append(columns_title)

rapports = {
    'liste_des_anomalies_sur_support' : {
    	'liste' : [],
	    'column': columns_title,
        'cpteur' : 0
    },
    'liste_des_anomalies_sur_format' : {
	    'liste' : [],
	    'column': columns_title,
        'cpteur' : 0
    },
    'liste_pour_reloc_NZ' : {
        'liste' : [],
        'column': ['mms_id'],
        'cpteur' : 0
    },
    'liste_des_anomalies_api' : {
        'liste' : [],
        'column':columns_title,
        'cpteur' : 0
    },
    'liste_des_ppns' : {
        'liste' : [],
        'column': ['PPN'],
        'cpteur' : 0
    }
    
}

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
                # We read the file
                group_list = []
                cpteur_preferred = 0
                return True, list(reader)
    except Exception as e:
        log_module.error("{}::Impossible de traiter le fichier::{}".format(file_name,str(e)))
        return False, []

def resource_type_analysis(rows) :
    nb_elec = 0
    for row in rows :
        resource_type = row[7]
        # log_module.debug(resource_type)
        if re.search(r"- Electronic$",resource_type):
            # log_module.debug("contient Electronic")
            nb_elec += 1
    return nb_elec

def preferred_record_analysis(rows) :
    nb_pref = 0
    for row in rows :
        operation = row[4]
        # log_module.debug(operation)
        if operation == 'preferred':
            # log_module.debug("contient preferred")
            nb_pref += 1
    return nb_pref

def preferred_record_definition(rows) :
    log_module.debug("preferred_record_definition")
    index = 0
    preferred_record = False 
    for index,row in enumerate(rows) :
        mms_id = row[1].replace("'","")
        # mms_id = row[1]
        log_module.debug(mms_id)
        doc = AlmaRecord.AlmaRecord(mms_id,apikey=API_KEY, service=SERVICE)
        log_module.debug(doc.error_status)
        if doc.error_status :
            return "Api_Error",rows
        # Si j'ai une notice Unimarc dans le groupe alors j'exclue le groupe de l'analyse
        log_module.debug("{} Notice unimarc : {}".format(mms_id,doc.is_unimarc_record()))
        if doc.is_unimarc_record() :
            return "Format_Error",rows
        # Si c'est une notice de la CZ on récupère le mmsid
        if doc.is_cz_record() :
            log_module.debug("{} est une notice de la CZ".format(mms_id))
            rapports['liste_pour_reloc_NZ']['liste'].append([mms_id])
        # Si j'ai déjà défini une notice préférée dans le groupe, je passe toutes les autres à merge
        if preferred_record == True :
            row[4] = 'merge'
        # Si je n'ai pas défini de notices préférés et que je suis sur ma dernière notice du groupe je la considère comme notice préférée
        elif index == len(rows) :
            row[4] = 'preferred'
        # Sinon je teste le système d'origine si celui-ci = ABES ou contient SUDOC alors je considère que c'est ma notice péférée
        else :
            if doc.is_abes_record() :
            # TODO Tese syst. d'origine
                row[4] = 'preferred'
                preferred_record = True
            else :
                row[4] = 'merge'    
    return "Ok",rows

# On récupère le fichier
statut, rows_list = file_in_array(file_name)
if not statut :
    log_module.error("ARRET DU TRAITEMENT")
    exit(1)
log_module.info("Fichier traité")

#On va regrouper nos lignes en fonction du groupe de traitement
# On créé un dictionnaire avec une clef par numéro de groupe et une lsite vide pour chaque entrée
pivot = {i[0]: { "ppn":i[2],"liste":[]} for i in rows_list}
for row in rows_list:
    #On ajoute nos lignes dans chaque entrée correspondante
    pivot[row[0]]["liste"].append(row)
log_module.info("Regroupent des lignes par numéro de groupe : OK")
# log_module.debug(pivot)

# Traitement des cas 
log_module.info("Début de l'analyse des données")
for key, values in pivot.items() :
    ppn = values['ppn']
    log_module.info("Traitement du groupe {} pour le ppn {}".format(key,ppn))
    nb_pref = 0
    # On Exclu le groupe si le PPN n'est pas valide
    if not(re.search(r"^\(ppn\)\d{8}[0-9xX]",ppn)):
        log_module.info("Le groupe {} pour le ppn {} a été ignoré car le PPN est invalide.".format(key,ppn))

        continue
    # Analyse des données
    # On détermine si tous les membres du groupes sont des documents électroniques
    nb_elec = resource_type_analysis(values['liste'])
    # Si aucune des notices n'est une notice de document électronique, on ignore le groupe
    if nb_elec == 0:
        log_module.info("Le groupe {} pour le ppn {} a été ignoré car il ne correspondait pas à une notice électronique.".format(key,ppn))
        continue

    elif nb_elec != len(values['liste']) :
        rapports['liste_des_anomalies_sur_support']['liste'].extend(values['liste'])
        log_module.info("Le groupe {} pour le ppn {} a été ajoutée à la liste des groupes à contrôler car une ou plusieurs notice n'était pas une notice électronique.".format(key,ppn))
        continue 
    else:
        # On regarde s'il ya bien une seule notice préféréé notice
        nb_pref = preferred_record_analysis(values['liste']) 
        log_module.debug("{}:{}".format(key,nb_pref))
        # S'il y a èune notice préférée on envoi 
        if nb_pref == 1 :
            liste_des_cents.extend(values['liste'])
            rapports['liste_des_ppns']['liste'].append([ppn.replace("(ppn)","")])
            nb_group +=1
        # Sinon on détermine la notice préférée
        else :
            statut, liste = preferred_record_definition(values['liste'])
            if statut == 'Api_Error' :
                log_module.info("Impossible de déterminer la notice préférée pour le groupe {} pour le ppn {}  suite à une erreur API.".format(key,ppn))                    
                rapports['liste_des_anomalies_api']['liste'].extend(values['liste'])
                continue
            if statut == 'Format_Error' :
                log_module.info("Le groupe {} pour le ppn {}  contient une notice unimarc.".format(key,ppn))                    
                rapports['liste_des_anomalies_sur_support']['liste'].extend(values['liste'])
                continue
            else :
                liste_des_cents.extend(liste)
                rapports['liste_des_ppns']['liste'].append([ppn.replace("(ppn)","")])
                nb_group +=1
    # Si j'ai traité 100 groupes, je créé un fichier avec mes candidats à la fusion. Le traitement ne traite que des lots de 100 groupes
    if nb_group%100 == 0 :
        with open('{}notices_a_fusionner_{}.csv'.format(REP,num_fichier), 'w') as f:
            mywriter = csv.writer(f, delimiter=',')
            mywriter.writerows(liste_des_cents)
            liste_des_cents = []
            liste_des_cents.append(columns_title)
            num_fichier +=1    
        log_module.debug("Ok")
# #  On envoie les dernières notices dans un fichier
with open('{}notices_a_fusionner_{}.csv'.format(REP,num_fichier), 'w') as f:
    mywriter = csv.writer(f, delimiter=',')
    mywriter.writerows(liste_des_cents)

# Rédaction des autres rapports
for file_name, rapport in rapports.items() :
    rapport['liste'].insert(0,rapport['column'])
    with open('{}{}.csv'.format(REP,file_name), 'w') as f:
        mywriter = csv.writer(f, delimiter=',')
        mywriter.writerows(rapport['liste'])
        log_module.info("{}:{}".format(file_name,len(rapport['liste'])-1))

log_module.info("FIN DU TRAITEMENT")
