import string
from collections import defaultdict
from functools import reduce

def sanitize_document(document):
    # Eliminar signos de puntuación
    for punctuation in string.punctuation:
        document = document.replace(punctuation, " ")
    return document  
    
# Mapper: genera pares (bigrama, documento) para cada bigrama en un documento
def mapper_helper(document_name, file_path):
    sub_result = {}
    with open(file_path+'/'+document_name, 'r', encoding='utf-8') as f:
        document = f.read()
        print("Sanitizing document "+document_name+"...")
        document = sanitize_document(document)
        print("Done sanitizing document "+document_name)
        words = document.split()
        bigrams = zip(words, words[1:])
        for a, b in bigrams:
            # a default value is returned if the key is not in the dictionary
            sub_result[tuple([a, b])] = sub_result.get(tuple([a, b]), set()) | set([document_name])
    # print("Mapper helper result: "+str(sub_result)[0:100])
    return sub_result

def mapper(document_names, file_path):
    result = {}
    for document_name in document_names:
        temp = mapper_helper(document_name, file_path)
        for key in temp:
            result[key] = result.get(tuple(key), set()) | temp[key]
    return result


# Reducer: combina los documentos asociados a cada bigrama en una lista
def reducer(result, sub_result):
    for key in sub_result:
        if result is not None:
            result[key] = result.get(tuple(key), set()) | sub_result[key]
        else:
            result = sub_result
    return result
