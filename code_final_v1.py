# Test In the Memory par Lina MEZIANE
# Version 1.0 16/12/2022

# on importe le module time qui servira pour calculer le temps d'exécution
import time

debut = time.time() # heure de début d'exécution

# on lit le fichier cs.txt pour obtenir la connection string et se connecter au Azure Storage Blob pour récupérer les fichiers
try:
    with open('cs.txt') as f:
        print("lecture du fichier cs.txt...")
        conn_str = f.readlines()
        conn_str = conn_str.pop(0) # readlines renvoie une liste donc on extrait le premier élément de cette liste pour avoir notre connection string

        from azure.storage.blob import BlobServiceClient
        # on crée un objet BlobServiceClient qui va nous permettre d'accéder aux containers présents sur cette connection
        blob_svc = BlobServiceClient.from_connection_string(conn_str)

        # on liste les containers et pour chacun, on liste les blobs présents dedans
        containers = blob_svc.list_containers()

        for c in containers:
            container_client = blob_svc.get_container_client(c)
            blob_list = container_client.list_blobs()
            for blob in blob_list: # on télécharge chaque blob qu'on va lire et écrire dans un fichier portant le nom du blob en question
                blob_client_instance = blob_svc.get_blob_client(c.name, blob.name, snapshot=None)
                print('téléchargement du blob : ' + blob.name)
                blob_data = blob_client_instance.download_blob()
                data = blob_data.readall()
                with open(blob.name, "wb") as file:
                    print('création du fichier : ' + blob.name)
                    file.write(data)

        # on importe la librairie sqlite3 qui va nous servir à interroger le système de gestion de base de données
    import sqlite3
        
    print('création/connexion à la base de données...')
    conn = sqlite3.connect('test_database.db') # on crée une nouvelle base de données si elle n'existe pas puis on s'y connecte
    c = conn.cursor()

    # on crée toutes les tables dont on va avoir besoin 
    # on vide nos tables à chaque exécution du script car on les alimente en full à chaque fois (on fait du truncate/insert)
    print('création de la table Clients...')
    c.execute('''
            CREATE TABLE IF NOT EXISTS Clients
            ([id] INTEGER, [name] TEXT, [job] TEXT, [email] TEXT, [account_id] INTEGER, PRIMARY KEY(id, name))
            ''') # par soucis d'unicité de la clé, on a décidé de mettre une double clé primaire car certains clients avaient le même id  
    c.execute('''
            DELETE FROM Clients
            ''')       
    print('création de la table Stores...')        
    c.execute('''
            CREATE TABLE IF NOT EXISTS Stores
            ([id] INTEGER PRIMARY KEY, [latlng] TEXT, [latitude] REAL, [longitude] REAL, [opening] TEXT, [closing] TEXT, [type] INTEGER)
            ''')
    c.execute('''
            DELETE FROM Stores
            ''')
    print('création de la table Products...')
    c.execute('''
            CREATE TABLE IF NOT EXISTS Products
            ([id] INTEGER PRIMARY KEY, [ean] INTEGER, [brand] TEXT, [description] TEXT)
            ''')
    c.execute('''
            DELETE FROM Products
            ''')
    print('création de la table Transactions...')
    c.execute('''
            CREATE TABLE IF NOT EXISTS Transactions
            ([transaction_id] INTEGER, [client_id] INTEGER, [account_id] INTEGER, [date] TEXT, [hour] INTEGER, [minute] INTEGER, [timestamp] TEXT, [product_id] INTEGER, [quantity] INTEGER, [store_id] INTEGER, PRIMARY KEY(transaction_id,client_id,account_id))
            ''') # comme pour la table Clients, on a créé une clé primaire multiple car certains transaction_id revenaient plusieurs fois, comme les client_id donc on a également l'account_id
    c.execute('''
            DELETE FROM Transactions
            ''')                           
    conn.commit()

    # on importe le module glob qui va nous permettre de lister les fichiers d'un même type dans un répertoire pour récupérer tous nos csv 
    import glob
    # on importe le module pandas pour analyser et manipuler nos données plus facilement à l'aide d'une structure de données appelée DataFrame 
    import pandas as pd

    df_clients = pd.DataFrame()
    df_products = pd.DataFrame()
    df_stores = pd.DataFrame()
    df_transactions = pd.DataFrame()

    csv_files = glob.glob('*.{}'.format('csv'))

    # on convertit nos fichiers csv en dataframe, en concaténant tous les fichiers avec la même structure 
    print('création du dataframe df_clients...')   
    df_clients = pd.concat([pd.read_csv(f,delimiter=';') for f in csv_files if 'clients' in f], ignore_index=True)
    df_clients.rename(columns = {'id':'client_id'}, inplace = True) # on renomme cette colonne car on va faire une jointure dessus entre les dataframes clients et transactions ensuite

    print('création du dataframe df_transactions...')
    df_transactions = pd.concat([pd.read_csv(f,delimiter=';',skiprows=[0],names=['transaction_id','client_id','date','hour','minute','product_id','quantity','store_id']) for f in csv_files if 'transactions' in f], ignore_index=True)
    df_transactions = pd.merge(df_transactions, df_clients[["client_id", "account_id"]], on="client_id", how="left") # jointure pour avoir l'account_id dans la table transactions
        
    df_clients.rename(columns = {'client_id':'id'}, inplace = True) # on renomme pour avoir le même champ qu'en table                 
    print('alimentation de la table Clients...')
    df_clients.to_sql("Clients",conn,if_exists='append', index=False) # on insert les lignes du dataframe dans la table correspondante 

    df_transactions = df_transactions.loc[(df_transactions["transaction_id"] != 'transaction_id') & (df_transactions["client_id"] != 'client_id')] # on enlève les lignes correspondant aux entêtes des fichiers csv
    df_transactions['time'] = pd.to_datetime(df_transactions['hour'].astype(str) + ':' + df_transactions['minute'].astype(str), format='%H:%M').dt.time # conversion en format datetime 
    df_transactions['timestamp']=pd.to_datetime(df_transactions['date'] + df_transactions['time'].apply(str), format='%Y-%m-%d%H:%M:%S') # création du champ timestamp 
    df_transactions = df_transactions[['transaction_id', 'client_id', 'account_id', 'date', 'hour','minute','timestamp','product_id','quantity','store_id']] # on réorganise l'ordre des colonnes
    print('alimentation de la table Transactions...')
    df_transactions.to_sql("Transactions",conn,if_exists='append', index=False)

    print('création du dataframe df_products...')
    df_products = pd.concat([pd.read_csv(f,delimiter=';') for f in csv_files if 'products' in f], ignore_index=True)

    print('alimentation de la table Products...')
    df_products.to_sql("Products",conn,if_exists='append', index=False)

    print('création du dataframe df_stores...')
    df_stores = pd.concat([pd.read_csv(f,delimiter=';') for f in csv_files if 'stores' in f], ignore_index=True)

    df_stores[['latitude','longitude']] = df_stores.latlng.str.split(",",expand=True) # on divise le champ latlng en deux champs latitude et longitude 
    df_stores["latitude"] = df_stores["latitude"].str.replace("(","")
    df_stores["longitude"] = df_stores["longitude"].str.replace(")","")
    df_stores['opening'] = pd.to_datetime(df_stores['opening'].astype(str), format='%H').dt.time # conversion au format datetime pour être cohérent avec la table transactions qui a des formats datetime
    df_stores['closing'] = pd.to_datetime(df_stores['closing'].astype(str), format='%H').dt.time 
    df_stores= df_stores[['id', 'latlng', 'latitude', 'longitude', 'opening','closing','type']]
    print('alimentation de la table Stores...',flush=True)
    df_stores.to_sql("Stores",conn,if_exists='append', index=False)

    import os
    for f in csv_files:
        os.remove(f) # on supprime les csv pour libérer de l'espace dans le répertoire

except Exception as e:
    print(e)
    


duree = time.time() - debut
print(f"Temps d'exécution: {duree} secondes")

