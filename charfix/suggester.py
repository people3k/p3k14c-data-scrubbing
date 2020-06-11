from symspellpy import SymSpell, Verbosity

sym_spell = SymSpell(max_dictionary_edit_distance=5)
sym_spell.create_dictionary('corpus/corpus.txt')

print('Enter in a word to receive correction suggestions')
while (True):
    print('')
    word = input('Word: ')
    suggestions = sym_spell.lookup(word, Verbosity.CLOSEST, max_edit_distance=5)
    print('Suggestion(s):')
    for sugg in suggestions:
        print(sugg)
