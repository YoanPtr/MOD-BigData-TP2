import sys, os 
import re
import string

def reducer(result_dic, output_filename):
    inversed_dict = {}

    # Inverser le dictionnaire pour regrouper les mots ayant la même signature de lettre
    for word, lst in result_dic.items():
        tuple_key = tuple(lst)
        if tuple_key in inversed_dict:
            inversed_dict[tuple_key].append(word)
        else:
            inversed_dict[tuple_key] = [word]
    
    # Ouverture du fichier en mode écriture pour stocker les résultats
    with open(output_filename, 'w') as fileOut:
        for key, value in inversed_dict.items():
            if len(value) > 1:
                result_line = f"{', '.join(value)}\n"
                fileOut.write(result_line)  
                print(result_line.strip())  

def word_to_list(word):
    alphabet = string.ascii_lowercase
    letter_count = [0] * 26
    for letter in word:
        index = alphabet.index(letter)
        letter_count[index] += 1
    return letter_count

def mapper(filename_original):
    
    fileIn = open(filename_original, 'r')

    result_dic = {}
    for word in fileIn:

        word = word.strip()
        word = word.lower()
        word = re.sub("[^A-Za-z]", " ", word)

        result_dic[word] = word_to_list(word)
    
    fileIn.close() 

    return result_dic

if __name__ == '__main__':
    
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

    filename = 'words_alpha.txt'
    output_filename = 'result.txt'  

    result_dic = mapper(filename)
    
    reducer(result_dic, output_filename)
