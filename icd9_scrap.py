from pyquery import PyQuery
from pkl_utils import *
import itertools as it
import re
import code
import sys

import gzip
import csv
import collections

###
# Scraps the ontology of ICD-9 codes from Wikipedia
###

# Important: this is not a hyphen, it's a "–"
level_exp = re.compile(r'\(([0-9]{3})[–-]([0-9]{3})\)')
E_level_exp = re.compile(r'\(E([0-9]{3})[0-9.]{0,3}[–-]E([0-9]{3})[0-9.]{0,3}\)')
E_level_onenum_exp = re.compile(r'\(E([0-9]{3})[0-9.]{0,3}\)')

def process_page(pq, tree, level):
    for elem in pq(".toclevel-{:d}".format(level)):
        elem = PyQuery(elem)
        texts = elem.children("a .toctext")
        if len(texts) > 1:
            import pdb
            pdb.set_trace()
        text = texts[0].text
        if level_exp.search(text):
            sub_tree = []
            process_page(elem("ul"), sub_tree, level+1)
            tree.append((text, sub_tree))

@memoize_pickle('basic_tree.pkl.gz')
def get_basic_tree():
    WIKI_BASE="https://en.wikipedia.org"
    d = PyQuery(url=WIKI_BASE+"/wiki/List_of_ICD-9_codes")
    links = d("#mw-content-text > ul:first-of-type > li > a")
    tree = []
    for link in links:
        pq = PyQuery(url=WIKI_BASE+link.get('href'))("#toc")
        process_page(pq, tree, 1)
    return tree

def get_e_tree():
    e_tree = []
    pq = PyQuery(url='https://en.wikipedia.org/wiki/List_of_ICD-9_codes_E_and_V_codes:_external_causes_of_injury_and_supplemental_classification')
    for elem in pq("#toc .toclevel-1 > a .toctext"):
        ELEs = E_level_exp.search(elem.text)
        if ELEs:
            gs0, gs1 = map(int, ELEs.groups())
            e_tree.append(("({:03d}-{:03d})".format(gs0, gs1), []))
        ELOEs = E_level_onenum_exp.search(elem.text)
        if ELOEs:
            gs0 = gs1 = int(ELOEs.group(1))
            e_tree.append(("({:03d}-{:03d})".format(gs0, gs1), []))
    return e_tree

def check_tree_in_range(tree, start, end, level=1, interact=False):
    num_tree = []
    for node in tree:
        gs = level_exp.search(node[0]).groups()
        n_start, n_end = map(int, gs)

        if n_start == 303:
            missed_by_parser = [300, 301, 302]
            num_tree += list(zip(missed_by_parser, missed_by_parser, it.repeat([])))

        # Add divisions missed by the parser
        if n_start == 570 and n_end == 579:
            node[1].extend([
                ("Liver (570-573)", []),
                ("Gallbladder (574-575)", []),
                ("Other biliary tract (576-576)", []),
                ("Other (577-579)", []),
                ])
        if n_start == 360 and n_end == 379:
            node[1].extend([
                ("Disorders of the globe (360-360)", []),
                ("Retinal disorders (361-362)", []),
                ("Chorioretinal inflammations, scars, and other disorders of choroid (363-363)", []),
                ("Disorders of iris and ciliary body (364-364)", []),
                ("Glaucoma (365-365)", []),
                ("Cataract (366-366)", []),
                ("Disorders of refraction and accommodation (367-367)", []),
                ("Visual disturbances (368-368)", []),
                ("Blindness and low vision (369-369)", []),
                ("Disorders of cornea (370-371)", []),
                ("Adnexa (372-375)", []),
                ("Disorders of the orbit (376-376)", []),
                ("Disorders of optic nerve and visual pathways (377-377)", []),
                ("Strabismus and other disorders of binocular eye movements (378-378)", []),
                ("Other (379-379)", []),
                ])

        if n_start == 764 and n_end == 779:
            node[1].extend([
                ("Length of gestation and fetal growth (764-766)", []),
                ("Birth trauma (767-767)", []),
                ("Hypoxia/asphyxia/respiratory (768-770)", []),
                ("Infections (771-771)", []),
                ("Hemorrhage (772-776)", []),
                ])
            for i in range(777, 779+1):
                node[1].append(("({:d}-{:d})".format(i, i), []))

        if len(num_tree) > 0:
            if n_start != num_tree[-1][1]+1:
                print("Number failed to be continuation", n_start, num_tree[-1][1])
                if interact:
                    code.interact(local=locals())
        else:
            if n_start != start:
                print("Tree doesn't start as supposed to", start, n_start)
                if interact:
                    code.interact(local=locals())
        num_tree.append((n_start, n_end,
                         check_tree_in_range(node[1], n_start, n_end, level+1,
                                             interact)))
    if len(num_tree) > 0:
        if end != n_end:
            print("Tree doesn't end as supposed to", end, n_end)
            if interact:
                code.interact(local=locals())
    num_tree.sort()
    return num_tree

@memoize_pickle('all_tree.pkl.gz')
def all_tree():
    tree = get_basic_tree()
    num_tree = check_tree_in_range(tree, 1, 999)
    print('========')
    e_tree = get_e_tree()
    e_num_tree = check_tree_in_range(e_tree, 1, 999)
    _v_num_tree = [
        (1,6), (7,9), (10,19), (20,29), (30,39), (40,49),
        (50,59), (60,69), (70,82), (83,84),
        (85,85), (86,86), (87,87), (88,88), (89,89), (90,90),
        (91,91),
    ]
    v_num_tree = list(zip(*zip(*_v_num_tree), it.repeat([])))
    return num_tree, e_num_tree, v_num_tree

def find_n(n, ls):
    a=0; b=len(ls)
    while a < b:
        i = (a+b)//2
        if ls[i][1] < n:
            a = i+1
        else:
            b = i
    assert ls[a][0] <= n <= ls[a][1], "digit {:d} not in interval [{:d}, {:d}]".format(
        n, ls[a][0], ls[a][1])
    return a

def find_icd9(code, trees, leaf_dict):
    if code[0] == 'E':
        code_before = code[1:4]
        code_after = code[4:]
        indices = [1]
    elif code[0] == 'V':
        code_before = code[1:3]
        code_after = code[3:]
        indices = [2]
    else:
        code_before = code[:3]
        code_after = code[3:]
        indices = [0]
    tree = trees[indices[0]]
    code_before = int(code_before)
    while len(tree) > 0:
        i = find_n(code_before, tree)
        indices.append(i)
        prev_tree = tree
        tree = tree[i][2]

    assert len(code_after) <= 2
    leaf_indices = []
    if len(code_after) > 0:
        idx = tuple(indices)
        if idx not in leaf_dict:
            leaf_dict[idx] = {}
        tree = leaf_dict[idx]
        while len(code_after) > 0:
            c, *code_after = code_after
            c = int(c)
            if c not in tree:
                tree[c] = (len(tree), {})
            leaf_indices.append(tree[c][0])
            tree = tree[c][1]

    return indices, leaf_indices

@memoize_pickle('codes_in_mimic.pkl.gz')
def codes_in_mimic():
    codes = set()
    with gzip.open('DIAGNOSES_ICD.csv.gz', 'rt') as f:
        r = iter(csv.reader(f))
        next(r)
        for row in r:
            codes.add(row[-1])
    return codes

def generate_all_indices(tree):
    all_indices = []
    for i in range(len(tree)):
        if len(tree[i][2]) == 0:
            all_indices.append((i,))
        else:
            for j in generate_all_indices(tree[i][2]):
                all_indices.append((i,) + j)
    return all_indices

def generate_level_sizes(tree, leaf_dict, level_sizes, condensed_tree, indices=()):
    level = len(indices)
    if len(tree) == 0:
        if indices in leaf_dict:
            level_sizes[level].update([len(leaf_dict[indices])])
            leaves = list(i for _, i in leaf_dict[indices].items())
            leaves.sort()
            for leaf in leaves:
                assert isinstance(leaf[1], dict), "We didn't unwittingly use the tuple"
                level_sizes[level+1].update([len(leaf[1])])
                assert sum(len(el[1]) for _, el in leaf[1].items()) == 0, "Tree two levels deep here"

                ct = [None]*len(leaf[1])
                condensed_tree.append(ct)
    else:
        level_sizes[level].update([len(tree)])
        for i, t in enumerate(tree):
            ct = []
            generate_level_sizes(t[2], leaf_dict, level_sizes, ct, indices+(i,))
            condensed_tree.append(ct)

# The unused buckets if we don't delete any
PREV_UNUSED_BUCKETS = [(0, 13, 6), (1, 24), (2, 14), (0, 0, 11)]
@memoize_pickle("condensed_tree.pkl.gz")
def get_condensed_tree():
    trees = all_tree()
    trees[1].insert(0, (0, 0, []))

    PREV_UNUSED_BUCKETS.sort(reverse=True)
    for idx in PREV_UNUSED_BUCKETS:
        tree = trees[idx[0]]
        for i in idx[1:-1]:
            tree = tree[i][2]
        del tree[idx[-1]]

    codes = codes_in_mimic()
    inds = set()
    code_to_index = {}
    leaf_dict = {}
    for c in filter(lambda c: c != "", codes):
        try:
            indices, leaf_indices = find_icd9(c, trees, leaf_dict)
            inds.add(tuple(indices))
            code_to_index[c] = tuple(indices+leaf_indices)
        except AssertionError as e:
            print("error in '{:s}'".format(c))
            print(e)
    print("There are codes in", len(inds), "buckets")

    _trees = list(zip(it.repeat(None), it.repeat(None), trees))
    all_codes = set(generate_all_indices(_trees))
    print("There are ", len(all_codes), "buckets")
    assert len(inds.difference(all_codes)) == 0
    print("Unused buckets:", all_codes.difference(inds))

    level_sizes = []
    for _ in range(10):
        level_sizes.append(collections.Counter())
    condensed_tree = []
    generate_level_sizes(_trees, leaf_dict, level_sizes, condensed_tree)
    print(level_sizes)
    return condensed_tree, code_to_index

if __name__ == '__main__':
    condensed_tree, code_to_index = get_condensed_tree()
    leaves = 0
    nodes = 0
    for _, idx in code_to_index.items():
        t = condensed_tree[idx[0]]
        for i in idx[1:]:
            t = t[i]
        if t is None:
            leaves += 1
        else:
            nodes += 1
    print(leaves, nodes)
