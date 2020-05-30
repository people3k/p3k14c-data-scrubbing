from symspellpy import SymSpell, Verbosity

sym_spell = SymSpell()
corpus_path = 'specials.txt'
sym_spell.create_dictionary(corpus_path)

input_term = 'bai'

suggestions = sym_spell.lookup(input_term, Verbosity.CLOSEST)

for suggestion in suggestions:
    print(suggestion)


# Prompt the user to fix it 
    # List context and suggestions
    # Prompt to:
        # [#] Choose suggestion
            # Prompt to fix [o]nce
                # Change the data structure in-place
            # Prompt to fix [a]ll
                # Add to fix-all dictionary
        # [F]lag for expert
            # Change the data-structure in-place
        # [L]eave alone
            # Skip
        # [M]anual entry
            # Take manual entry
            # Change structure in-place


# For every fixable field
    # If the string contains non-alphanumeric characters and isn't in the fix-all dictionary
        # Prompt the user to fix it
# Now, apply everything in the fix-all dictionary
