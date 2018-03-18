from random import randrange

randword = lambda x : ''.join(chr(randrange(98,123)) for _ in range(x))