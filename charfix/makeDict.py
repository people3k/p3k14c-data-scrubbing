import nltk
import os

LATIN_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ\
               abcdefghijklmnopqrstuvwxyz\
               0123456789\
               ±°[]{}\|`~!@#$%^&*()\\-+_=,./<>?:;"\'\
               ·½\
               '

def tokenize(filename):
        content = open(filename, 'rb').read().decode('utf-8')
        tokens = nltk.word_tokenize(str(content))
        tokens = [ t.split(',')[0] for t in tokens]
        uniqueTokens = list(set(tokens))
        return uniqueTokens
 

def special(token):
    for c in token:
        if c not in LATIN_CHARS:
            return True
    return False

def getSpecials(tokens):
    return [t for t in tokens if special(t)]
 
def main():
    i = 0
    for file in os.listdir('exper/'):
        tokens = tokenize('exper/'+file)
        specials = getSpecials(tokens)
        print(specials)
        i +=1
        if (i >= 3):
            exit()


if __name__ == '__main__':
    main()
