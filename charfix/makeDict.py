import nltk
import ftfy
import os

LATIN_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ\
               abcdefghijklmnopqrstuvwxyz\
               0123456789\
               ±°[]{}\|`~!@#$%^&*()\\-+_=,./<>?:;"\'\
               −\x80 \xad ·“”½–’‘:—▼−—\
               '

def tokenize(filename):
        content = open(filename, 'rb').read().decode('utf-8')
        tokens = nltk.word_tokenize(str(content))
        tokens = [ ftfy.fix_encoding(t.split(',')[0]) for t in tokens]
        return tokens
 

def special(token):
    for c in token:
        if c not in LATIN_CHARS:
            return True
    return False

def getSpecials(tokens):
    return [t for t in tokens if special(t)]
 
def main():
    allSpecials = []
    for file in os.listdir('exper/'):
        tokens = tokenize('exper/'+file)
        specials = getSpecials(tokens)
        allSpecials += specials
        print(specials)
    allSpecials = ' '.join(allSpecials)
    with open('specials.txt', 'w') as f:
        f.write(allSpecials)



if __name__ == '__main__':
    main()
