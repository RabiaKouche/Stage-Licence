#################################################################################
### 
### Programme d'alimentation de la base de données 'Navires' comprenant
### deux tables : (1) Navire ; (2) Arborescence_navires
### Auteurs : Rabia KOUCHE (L3 Informatique - 2019-2020)
###           Claude DUVALLET
### Version : 02/05/2020
###
#################################################################################

# Il faut télecharger et installer le module 'psycopg2' pour pouvoir 
# se connecter à une base de données PostGreSQL depuis un programme Python

import glob
import time
import psycopg2 as psycopg2

# Debut du decompte du temps


start_time = time.time()

print("##############################################################")
print("### Programme d'alimentation de la base de données navire  ###")
print("### Importation de données au format CSV dans une base de  ###")
print("### données PostGreSQL                                     ###")
print("##############################################################")

# Connexion a la base de données bd_navire
# Attention, il faut que l'utilisateur soit le propriétaire de la base de données
# ou possède des droits suffisant
connexionBD = psycopg2.connect(
    host="localhost",  # Nom du serveur où se trouve la base de données
    database="bd_navire",  # Nom de la base de données contenant déjà la structure
    # user = "USER",              # Nom de l'utilisateur
    # password = "PASSWORD"       # Mot de passe de l'utilisateur
    user="dev",  # Nom de l'utilisateur
    password="13041996"  # Mot de passe de l'utilisateur
)

curseur = connexionBD.cursor()

#################################################################################
### Alimentation de l'arborescence des navires sur 4 niveaux
### Le niveau 2 est lié au type de navire (nom des fichiers Excel)
### Le niveau 4 correspond à la colonne 'ship type'
#################################################################################

# Ouverture de fichier Arborescence navires.csv en mode lecture
with open("Arborescence navires.csv", 'r') as fp:
    next(fp)
    # La méthode readlines () lit jusqu'à la fin de fichier, et retourne une liste contenant les lignes
    lignes = fp.readlines()

nombre_ligne_importe = 0
nombre_ligne_en_double = 0

# Pour chaque ligne lu dans le fichier, on la découpe et on insère les champs dans la base de données    
for ligne in lignes:
    infos = ligne.split(";", 3)
    # insertion des données dans la table Arborescence navires
    infos[3].split(" ")
    try:
        curseur.execute("INSERT INTO Arborescence_navires (niveau1, niveau2, niveau3, niveau4) "
                        "VALUES(%s, %s, %s, %s)",
                        (infos[0].strip(), infos[1].strip(), infos[2].strip(), infos[3].strip().title()))

        nombre_ligne_importe += 1  # On compte le nombre de navires importés
        # except (Exception, psycopg2.DatabaseError) as error:
    except:
        # print(error)
        nombre_ligne_en_double += 1  # On compte le nombre de navires non importés car déjà présent dans la base

# La méthode commit () : enregistre toutes les modifications apportées depuis le dernier commit dans la base de données.
connexionBD.commit()
print("{:40s}: importation de {:7d} de lignes et  rejet de {:5d} lignes en double".format("Arborescence navires.csv",
                                                                                          nombre_ligne_importe,
                                                                                          nombre_ligne_en_double))

# Attente avant de passer à l'importation des navires :
# input()
# On liste l'ensemble des fichiers CSV présents dans le répertoire en supposant qu'ils sont au bon format
tableau_navire = []
for file in glob.glob("fichiersCSV/*.csv"):
    if file != "Arborescence navires.csv":
        # print ("Ajout de '{:45s}' à la liste des fichiers à traiter".format(file.replace(":","/")))
        tableau_navire.append(file)
nombre_total_navire = 0
# on parcours tout les fichiers pour les importer a bd_navire
for navire in tableau_navire:
    # ouverture d'un fichier CSV en mode lecture
    with open(navire, "r") as fp:
        # next(fp) : cette methode renvoie la ligne d'entrée suivante (la premiére ligne pour les noms de colonnes)
        next(fp)
        # cette méthode readlines () lit jusqu'à la fin de fichier, et retourne une liste contenant les lignes
        data = fp.readlines()

    # pour chaque ligne dans l'ensemble des lignes data
    nombre_navire_importe = 0
    nombre_navire_en_double = 0
    ship_type_inconnu = 0
    # On ne récupère que la partie centrale du nom de fichier
    # qui correspond au niveau 2 de l'arborescence
    partie_navire_nom = navire.split(".", 3)
    navire = partie_navire_nom[1]
    for d in data:
        # split () renvoie une liste de tous les mots de la chaîne il en exite 73, en utilisant ";" comme séparateur
        infos = d.split(";", 73)
        # Insertion des données dans la table navire
        # Pour les champs de type 'real', j'ai utilisé la méthode replace(",",".") 
        # car les données de type réel dans les fichiers sont représentés avec des virgules
        # J'ai aussi utilisé " or 0 " dans les champs entiers qui peuvent être null, 
        # le default 0 sur le fichier sql ne fonctionne pas.
        # Si jamais le champs 'IMO_LR_IHS_NO' existe déjà (navire déjà inséré dans la BD)
        # alors une erreur risque de se produire, c'est pourquoi il faut gérer les exceptions
        try:
            curseur.execute("INSERT INTO Navire (IMO_LR_IHS_NO, MMSI, Aux_Engine_Builder,"
                            " Aux_Engine_Design, Aux_Engine_Model, Aux_Engine_Stroke_Type, Aux_Engine_Total_KW,"
                            " Breadth, Breadth_Extreme, Breadth_Moulded, Built, Cabins, Call_Sign, Class, Country_Of_Build,"
                            " Dead_Weight, Delivery_date, Depth, Displacement, Doc_Company, Doc_Company_Code, Docking_Survey,"
                            " Draugth, Engine_Bore, Engine_Builder, Engine_Cylinders, Engine_Design, Engine_Model, Engine_Stroke,"
                            " Engine_Stroke_Type, Engine_Type, Engine_Number, Engines_RPM, Flag, Fluel_Capacity1, Fluel_Capacity2,"
                            " Fuel_Consumption_Main_Engines, Fuel_Consumption_Total, Fuel_Type1, Fuel_Type2, Gas_Capacity,"
                            " Group_Owner, Group_Owner_Code, GT, Keel_Laid, Keel_To_Mast_Height, Last_Update, Launch_Date,"
                            " Length, Name_of_ship, Operator, Operator_Code, Order_Date, Propulsion_Type, Reefer_Points,"
                            " Registered_Owner, Registered_Owner_Code, RORO_Lanes_Number, Sale_Date, Sale_Price_US,"
                            " Segregated_Ballast_Capacity, Service_Speed, Ship_Type, Ship_Type_Group, Shipbuilder,"
                            " Shipbuilder_Code, Shipmanager, Shipmanager_Code, Status, Technical_Manager,"
                            " Technical_Manager_Code, TEU, Year, Type_Of_Navire) VALUES "
                            "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                            " %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                            " %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                            " %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                            " %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                            " %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                            " %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                            " %s,%s,%s,%s)",
                            (infos[0] or 0, infos[49].replace(",", "."), infos[2],
                             infos[3], infos[4], infos[5] or 0, infos[6] or 0,
                             infos[7].replace(",", ".") or 0.0,
                             infos[8].replace(",", ".") or 0.0, infos[9].replace(",", ".") or 0.0, infos[10],
                             infos[11] or 0, infos[12], infos[13],
                             infos[14], infos[15] or 0,
                             infos[16], infos[17].replace(",", ".") or 0.0, infos[18] or 0, infos[19], infos[20] or 0,
                             infos[21] or 0, infos[22].replace(",", ".") or 0.0, infos[23] or 0, infos[24],
                             infos[25] or 0, infos[26], infos[27],
                             infos[28] or 0, infos[29] or 0, infos[30], infos[31], infos[32] or 0, infos[33],
                             infos[34].replace(",", ".") or 0.0,
                             infos[35].replace(",", ".") or 0.0, infos[36].replace(",", ".") or 0.0,
                             infos[37].replace(",", ".") or 0.0, infos[38],
                             infos[39], infos[40], infos[41], infos[42], infos[43], infos[44],
                             infos[45].replace(",", ".") or 0.0, infos[46], infos[47],
                             infos[48].replace(",", "."), infos[1], infos[50], infos[51] or 0, infos[52], infos[53],
                             infos[54],
                             infos[55], infos[56], infos[57], infos[58], infos[59], infos[60] or 0, infos[61],
                             infos[62].replace("/", " ").strip().title(), infos[63], infos[64],
                             infos[65], infos[66], infos[67] or 0, infos[68], infos[69], infos[70] or 0, infos[71] or 0,
                             infos[72], navire))

            nombre_navire_importe += 1  # On compte le nombre de navires importés
            connexionBD.commit()
        except psycopg2.errors.UniqueViolation as error:
            # Il existe des navires en double pouvant provoquer une violation de la clef primaire
            # Affichage des erreurs (à décommenter si nécessaire)

            """print("[ERREUR] TYPE  :",type(error))
          print("[ERREUR] ARGS  :",error.args)     
          print("[ERREUR]       :",error)
          exit()"""
            nombre_navire_en_double += 1  # On compte le nombre de navires non importés car déjà présent dans la base
            # print(nombre_navire_en_double,":",infos[0])
            # Il faut faire un rollback sur la transaction echouée sinon, cela bloquera toutes les transaction suivantes.
            connexionBD.rollback()
        except psycopg2.errors.ForeignKeyViolation as error:
            # Il existe des navires en double pouvant provoquer une violation de la clé étrangère présente dans
            # la table arborescence_navire et liée à la table navire
            # Affichage des erreurs (à décommenter si nécessaire)
            # On construit une liste des entrées manquantes dans le fichier "arborescence_navires"
            # et s'il en manque on affiche une erreur nous permettant d'identifier une nouvelle entrée manquante
            ListeNiveau4Manquant = []
            ListeNiveau4Manquant.append("Training Ship, Stationary")
            ListeNiveau4Manquant.append("Drilling Rig, Semi Submersible")
            ListeNiveau4Manquant.append("Drilling Rig, Jack Up")
            ListeNiveau4Manquant.append("Combination Gas Tanker (Lng Lpg)")
            ListeNiveau4Manquant.append("Accommodation Platform, Semi Submersible")
            ListeNiveau4Manquant.append("Accommodation Platform, Jack Up")
            ListeNiveau4Manquant.append("Submarine")
            ListeNiveau4Manquant.append("Shuttle Tanker")
            ListeNiveau4Manquant.append("Jacket Launching Pontoon, Semi Submersible")
            ListeNiveau4Manquant.append("Mooring Buoy")
            ListeNiveau4Manquant.append("")

            if not infos[62].replace("/", " ").strip().title() in ListeNiveau4Manquant:
                print("[ERREUR] TYPE  :", type(error))
                print("[ERREUR] ARGS  :", error.args)
                print("[ERREUR]       :", error)
                input()
            ship_type_inconnu += 1  # On compte le nombre de navires non importés car déjà présent dans la base
            # print(nombre_navire_en_double,":",infos[0])
            # Il faut faire un rollback sur la transaction echouée sinon, cela bloquera toutes les transaction suivantes.
            connexionBD.rollback()
            # exit()
        except Exception as error:
            print("[ERREUR] Exception :", error)
            print("ligne    =", infos[0])
            exit()

        except:
            print("[ERREUR] Erreur non répertorié")
            print("ligne    =", d)

    # La méthode commit () : enregistre toutes les modifications apportées depuis le dernier commit.
    connexionBD.commit()
    print("{:40s}: importation de {:7d} de navires et rejet de {:5d} navires en double et {:3d} ship type inconnu".
          format(navire.replace(":", "/"), nombre_navire_importe, nombre_navire_en_double, ship_type_inconnu))

    nombre_total_navire += nombre_navire_importe

print("Le nombre total de navires importés dans la base de données est {:d}".format(nombre_total_navire))

# Fermeture de la connexion
connexionBD.close()

# Affichage du temps d'exécution
print("Temps d'exécution : {:5.4f} secondes".format(time.time() - start_time))
