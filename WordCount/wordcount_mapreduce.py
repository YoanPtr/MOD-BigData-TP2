import sys, os 
import re

def reducer (filename_resultmappersorted, filename_resultreducer):

    # Ouverture en lecture du fichier
    fileIn = open(filename_resultmappersorted, 'r')
    # Ouverture en écriture du fichier contenant le resultat du reducer
    fileOut = open(filename_resultreducer, 'w')

    current_word = None
    current_count = 0
    word = None

    for line in fileIn:
        # remove leading and trailing whitespace
        line = line.strip()

        # parse the input we got from mapper.py
        word, count = line.split('\t', 1)
        
        # convert count (currently a string) to int
        try:
            count = int(count)
        except ValueError:
            # count was not a number, so silently
            # ignore/discard this line
            continue
        
        # this IF-switch only works because Hadoop sorts map output
        # by key (here: word) before it is passed to the reducer
        if current_word == word:
            current_count += count
        else:
            if current_word != None:
                fileOut.write(current_word+'\t'+str(current_count)+'\n')
            current_count = count
            current_word = word

    # do not forget to output the last word if needed!
    if current_word == word:
        fileOut.write(current_word+'\t'+str(current_count)+'\n')

    fileOut.close() # fermeture du fichier resultat
    fileIn.close() # fermeture du fichier original

def mapper(filename_original, filename_resultmapper):
    
    # Ouverture en lecture du fichier
    fileIn = open(filename_original, 'r')
    # Ouverture en écriture du fichier contenant le resultat du mapper
    fileOut = open(filename_resultmapper, 'w')
    
    for line in fileIn:
        # remove leading and trailing whitespace
        line = line.strip()
        # lower the line 
        line = line.lower()
        #Replace every non-alphabetic characters 
        line = re.sub("[^A-Za-z]","",line)
        # split the line into words
        words = line.split()
        # increase counters
        for word in words:
            # write the results to STDOUT (standard output);
            # what we output here will be the input for the
            # Reduce step, i.e. the input for reducer.py
            # tab-delimited; the trivial word count is 1
            fileOut.write(word+'\t'+'1\n')
    
    fileOut.close() # fermeture du fichier resultat
    fileIn.close() # fermeture du fichier original

if __name__ == '__main__':
    
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

    filename_original           = ['dracula','amph','2015']
    filename_resultmapper_base       = 'resultmapper'
    filename_resultmappersorted_base = 'resultmappersorted'
    filename_resultreducer_base      = 'resultreducer'

    # Appel au mapper
    for filename in filename_original:

        filename_resultmapper = filename_resultmapper_base + '_' + filename + '.txt'
        filename_resultmappersorted = filename_resultmappersorted_base + '_' +  filename + '.txt'
        filename_resultreducer = filename_resultreducer_base + '_' +  filename + '.txt'

        filename = filename + '.txt'

        mapper(filename, filename_resultmapper)
        
        # Tri du résultat précédent
        with open(filename_resultmapper,"r") as f:
            with open(filename_resultmappersorted, "w") as g:
                lines = f.readlines()
                g.writelines(sorted(lines))
        
        # Appel au reducer
        reducer(filename_resultmappersorted, filename_resultreducer)
