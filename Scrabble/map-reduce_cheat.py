import sys, os 
import re
import string
from collections import Counter
from collections import defaultdict

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
    return inversed_dict

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

def can_form(tuple_word, base_tuple):
    # Create a frequency dictionary for the input word


    # Check if each character in the word can be formed from base_tuple
    for i, count in enumerate(base_tuple):
        if count > tuple_word[i]:
            return False
    return True

from collections import defaultdict

def calculate_score(word, reference):
    """ Calcule le score d'un mot en fonction de sa longueur par rapport au mot de référence """
    return len(word) / len(reference)

def group_words_by_score(words, reference):
    """ Trie et regroupe les mots par score """
    score_groups = defaultdict(list)  # Dictionnaire pour regrouper les mots par score
    for word in words:
        score = calculate_score(word, reference)
        score_groups[score].append(word)
    
    # Trie les groupes par score décroissant et les mots de chaque groupe par ordre alphabétique
    sorted_groups = sorted(score_groups.items(), key=lambda x: -x[0])
    for score, group in sorted_groups:
        group.sort()  # Trie les mots alphabétiquement dans chaque groupe
    return sorted_groups

def process_words(words, reference):
    # Regroupe les mots par score
    sorted_groups = group_words_by_score(words, reference)
    
    # Affiche les groupes de mots avec leur score
    for score, group in sorted_groups:
        print(f"{score:.3f} {group}")
if __name__ == '__main__':
    
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

    filename = 'words_alpha.txt'
    output_filename = 'result.txt'  

    result_dic = mapper(filename)
    
    inverted_dic = reducer(result_dic, output_filename)

    # Example word to check
    word = 'ocarina'  

    tuple_word = tuple(word_to_list(word))
    
    results = []
    for base_tuple, words in inverted_dic.items():
        if can_form(tuple_word, base_tuple):
            results.extend(words)  # Use extend instead of += for clarity
    
    process_words(results, word)