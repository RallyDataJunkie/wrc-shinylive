import inflect

p = inflect.engine()

import random

def nth(n):
    return p.ordinal(n)

def Nth(n):
    return p.number_to_words(p.ordinal(n))

# Utility rule functions
def sometimes(t, p=0.5, prefix=""):
    """Sometimes return a string passed to the function."""
    if random.random() >= p:
        return f"{prefix}{t}"
    return ""


def occasionally(t):
    """Sometimes return a string passed to the function."""
    return sometimes(t, p=0.2)


def rarely(t):
    """Rarely return a string passed to the function."""
    return sometimes(t, p=0.05)


def pickone_equally(l, prefix="", suffix=""):
    """Return an item from a list,
    selected at random with equal probability."""
    t = random.choice(l)
    if t:
        return f"{prefix}{t}{suffix}"
    return suffix


def pickfirst_prob(l, p=0.5):
    """Select the first item in a list with the specified probability,
    else select an item, with equal probability, from the rest of the list."""
    if len(l) > 1 and random.random() >= p:
        return random.choice(l[1:])
    return l[0]
