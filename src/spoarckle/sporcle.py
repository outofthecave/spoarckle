# -*- coding: UTF-8 -*-

import re

"""
Sporcle-specific functions
"""

PATTERN_REMOVE = re.compile("[.!?'\"]+")
PATTERN_WHITESPACE = re.compile("[\\s/-]+")
PATTERN_SAINT = re.compile("\\bst\\b")
PATTERN_SUBD = re.compile("\\bsubd\\s+[a-z0-9]\\b")
PATTERN_NUMERO = re.compile("\\bno\\s+[0-9]+\\b")

REPLACE_CHAR = {
u"á": u"a",
u"à": u"a",
u"â": u"a",
u"é": u"e",
u"è": u"e",
u"ê": u"e",
u"í": u"i",
u"ì": u"i",
u"î": u"i",
u"ó": u"o",
u"ò": u"o",
u"ô": u"o",
u"ú": u"u",
u"ù": u"u",
u"û": u"u",
u"ý": u"y",
u"ỳ": u"y",
u"ŷ": u"y",
}

def tokenize(string):
    """
    Tokenize by whitspace and hyphens, expand a few abbreviations,
    ASCII-fold, lowercase, remove punctuation
    """
    string = string.lower()
    # remove punctuation
    string = PATTERN_REMOVE.sub("", string)
    # normalize whitspace
    string = PATTERN_WHITESPACE.sub(" ", string)
    # expand abbreviations
    string = PATTERN_SAINT.sub("saint", string)
    string = PATTERN_SUBD.sub("", string)
    string = PATTERN_NUMERO.sub("", string)
    # ASCII-fold
    string_builder = []
    for char in string:
        string_builder.append(REPLACE_CHAR.get(char, char))
    string = "".join(string_builder)
    return string.strip().split()
