# Analyse des titres dupliqués pour fusion des notices électroniques en doublon 
Ce programme prend en entrée le rapport fourni par le traitemnet Alma __Analyse de titre dupliqué__ (Ressources>Outils avancés>Analyse de titre dupliqué). 
Il analyse les groupes identifiés dans Alma et : 
 -  exclue les groupes de notices qui ne sont pas des notices électroniques ou des notices marc21. 
 - signale des groupes contenant :
   - à la fois des notices de documents électroniques et de document sur un aure support (rapport **liste_des_anomalies_sur_support**)
   - à la fois des notices marc21 et des notices unimarc (rapport **liste_des_anomalies_sur_format**)
 - identifie les notices de la CZ à copier dans la NZ (**liste_pour_reloc_NZ**)
 - fournie la liste des PPN des notices à recharger (**liste_des_ppns**)
 - prépare les fichiers pour le traitement Alma **Fusionner les notices et combiner l'inventaire**
   - Détermine la notice préférée : privilégie la notice SUDOC
   - Fait des lots de 100 groupes de doublons (le traitement de fusion est limité à 100 groupes de notices par fichier)