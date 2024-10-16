import sys, os
import shutil
def reducer(filename_resultmappersorted, filename_resultreducer):
    # Ouverture en lecture du fichier
    fileIn = open(filename_resultmappersorted, 'r')
    # Ouverture en écriture du fichier contenant le résultat du reducer
    fileOut = open(filename_resultreducer, 'w')

    current_word = None
    current_count = 0
    word = None

    for line in fileIn:
        # remove leading and trailing whitespace
        line = line.strip()

        # parse the input we got from mapper
        word, count = line.split('\t', 1)
        
        # convert count (currently a string) to int
        try:
            count = float(count)
        except ValueError:
            # count was not a number, so silently
            # ignore/discard this line
            continue
        
        # this IF-switch only works because Hadoop sorts map output
        # by key (here: word) before it is passed to the reducer
        if current_word == word:
            current_count += count
        else:
            if current_word is not None:
                fileOut.write(current_word + '\t' + str(current_count) + '\n')
            current_count = count
            current_word = word

    # do not forget to output the last word if needed!
    if current_word == word:
        fileOut.write(current_word + '\t' + str(current_count) + '\n')

    fileOut.close()  # fermeture du fichier résultat
    fileIn.close()   # fermeture du fichier original


def mapper(filename_original, columns_to_map, map_type):
    columns = ['Date', 'Hour', 'City', 'Category', 'Amount', 'Paiement']
    
    # Vérifier si les colonnes à mapper sont valides
    if not all(col in columns for col in columns_to_map):
        raise ValueError("Invalid column name in columns_to_map")

    # Ouvrir le fichier d'entrée en lecture
    fileIn = open(filename_original, 'r')


    # Créer un fichier de sortie pour le résultat du mapping
    # Le fichier prendra un nom basé sur les colonnes mappées et le type de mapping
    filename_result = os.path.join('Mapper', f'resultmapper_{"_".join(columns_to_map)}_{map_type}.txt')
    fileOut = open(filename_result, 'w')

    
    # Lire les lignes du fichier d'entrée
    for line in fileIn:
        # Supprimer les espaces en début et fin de ligne
        line = line.strip()

        # Séparer les colonnes par tabulation (\t)
        column_values = line.split('\t')
        
        #Changer l'heure afin de ne prendre que les deux premires charactères
        column_values[1] = column_values[1][:2]
        # Vérifier que la ligne contient bien le nombre correct de colonnes
        if len(column_values) == len(columns):
            # Générer la clé en combinant les colonnes à mapper
            key = '_'.join([column_values[columns.index(col)].strip() for col in columns_to_map])

            # Obtenir la valeur correspondante selon le type de mapping
            if map_type == 'count':
                value = '1'  # Comptage simple
            elif map_type == 'amount':
                value = column_values[columns.index('Amount')].strip()  # Utiliser la colonne Amount
            else:
                raise ValueError("Invalid map_type. Choose either 'count' or 'amount'.")

            # Écrire le résultat sous forme "clé\tvaleur" dans le fichier de sortie
            fileOut.write(key + '\t' + value + '\n')
    
    # Fermer le fichier de sortie et le fichier d'entrée
    fileOut.close()
    fileIn.close()


def map_sort_and_reduce(filename, columns_to_map, map_type):
    # Appel de la fonction mapper avec le type de map spécifié (count ou amount)
    mapper(filename, columns_to_map, map_type)

    # Nom des fichiers de sortie basé sur les colonnes à mapper et le type de mapping
    filename_mapper = os.path.join('Mapper', f'resultmapper_{"_".join(columns_to_map)}_{map_type}.txt')
    filename_sorted = os.path.join('Sorted', f'resultsorted_{"_".join(columns_to_map)}_{map_type}.txt')
    filename_reducer = os.path.join('Reducer', f'resultreducer_{"_".join(columns_to_map)}_{map_type}.txt')
    
    # Lire le fichier resultmapper et le trier
    with open(filename_mapper, "r") as f:
        with open(filename_sorted, "w") as g:
            lines = f.readlines()
            g.writelines(sorted(lines))
    
    # Appeler le reducer sur le fichier trié
    reducer(filename_sorted, filename_reducer)

def reset_directories(directories):
    for directory in directories:
        if os.path.exists(directory):
            shutil.rmtree(directory)  # Supprime le dossier et son contenu
        os.makedirs(directory, exist_ok=True)  # Crée le dossier


if __name__ == '__main__':
    
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)


    # Liste des répertoires à réinitialiser
    directories_to_reset = ['Mapper', 'Reducer', 'Sorted']

    # Réinitialiser les répertoires
    reset_directories(directories_to_reset)
    filename = 'purchases10000.txt'

    # Nombre pour chaque catégorie
    columns_to_map = ['Category']
    map_sort_and_reduce(filename, columns_to_map, 'count')
    
    # Somme pour chaque catégorie
    map_sort_and_reduce(filename, columns_to_map, 'amount')

    #Get information for San Francisco
    columns_to_map = ['City','Paiement']
    map_sort_and_reduce(filename, columns_to_map, 'amount')
    
    filename_result = os.path.join('Reducer', f'resultreducer_{"_".join(columns_to_map)}_{'amount'}.txt')

    # Ouvrir le fichier résultant en lecture
    with open(filename_result, 'r') as file:
        for line in file:
            # Vérifier si la ligne correspond à San Francisco
            if line.startswith("San Francisco"):
                print(line.strip())  # Afficher la ligne filtrée
                
    #Get information for San Francisco
    columns_to_map = ['Category','City','Paiement']
    map_sort_and_reduce(filename, columns_to_map, 'amount')

    filename_result = os.path.join('Reducer', f'resultreducer_{"_".join(columns_to_map)}_{'amount'}.txt')

    max_amount = float('-inf')  # Valeur minimale pour commencer
    max_line = None  # Ligne avec le montant maximum

    # Ouvrir le fichier résultant en lecture
    with open(filename_result, 'r') as file:
        for line in file:
            # Vérifier si la ligne correspond à Women's Clothing
            if line.startswith("Women's Clothing") and 'Cash' in line:
                # Extraire le montant
                parts = line.strip().split('\t')
                amount = float(parts[-1])  # Convertir le montant en float

                # Vérifier si c'est le maximum
                if amount > max_amount:
                    max_amount = amount
                    max_line = line.strip()  # Stocker la ligne avec le montant maximum

    # Afficher la ligne avec le montant maximum
    if max_line:
        print(max_line)

    columns_to_map = ['Hour']
    map_sort_and_reduce(filename, columns_to_map, 'count')
