import pickle
import csv
import re

ex_float = "([+-]?[0-9O]*[.]?[0-9O]*[.]?[0-9O]+)"

# These item ID are excluded so they can be cleaned manually
excluded_itemid = {
    # Below this before line skip they are all urine:
    51492, #Protein (urine)
    51514, #Urobilinogen
    51484, #Ketone
    51478, #Glucose (urine)
    51508, #Urine color
    51506, #Urine appearance
    51466, #Blood
    51464, #Bilirubin
    51487, #Nitrite
    51486, #Leukocytes
    51519, #Yeast
    51463, #Bacteria
    51512, #Urine mucous
    51462, #Amorphous crystals
    51518, #WBC clumps
    51469, #Calcium oxalate crystals
    51505, #Uric acid crystals

    51513, #Urine specimen type

    51523, #GR hold

    50880, #Benzodiazepine screen

    51266, #Platelet smear

    50975, #Protein Electrophoresis
    51098, #Protein Electrophoresis urine
    50948, #Immunofixation
    51086, #Immunofixation, urine

    50857, #Acetone (blood)
}

to_make_float = {
    51501, #Transitional Epithelial Cells
    50940, #Hepatitis B surface antibody
    51497, #Renal Epithelial Cells
    51076, #Bicarbonate (urine)
    50946, #Human Chorionic Gonadotropin
    51479, #Granular casts
}

def clean(s):
    s = s.replace('O', '0')
    while '..' in s:
        s = s.replace('..', '.')
    half = len(s)//2
    if s.count('.') > 1 and s[:half] == s[half:]:
        s = s[:half]
    if s.count('.') == 2:
        if s[0] == '.':
            s = s[1:]
        else:
            first_dot = s.find(".")
            s = s[:s.find(".", first_dot+1)]
    try:
        return float(s)
    except ValueError:
        import pdb
        pdb.set_trace()
    return 0.0

range_rex = re.compile("[^0-9]*"+ex_float+" ?- ?"+ex_float)
range_rex_2 = re.compile("[^0-9]*BETWEEN[^0-9]*"+ex_float+"[^0-9]*"+ex_float)
rex = [[[".*< *"+ex_float, ".*<? *LES[ LESTHAN]{0,10}"+ex_float], lambda v: clean(v)*0.9],
       [[".*> *"+ex_float, ".*>? *GR?EA[ GREATERHAN]{0,15}"+ex_float], lambda v: clean(v)*1.1],
       [[range_rex, range_rex_2], lambda a, b: (clean(a)+clean(b))/2],
       [["[^=]*= *"+ex_float], clean],
       [[" *"+ex_float], clean],
       [["POS"], lambda: 1],
       [["NEG"], lambda: -1], ]

for i in range(len(rex)):
    rex[i][0] = list(map(re.compile, rex[i][0]))

def clean_v(v):
    for rs, f in rex:
        for r in rs:
            if r.match(v) is not None:
                return f(*r.match(v).groups())
    return None

def correct_values(values, is_float, itemid):
    if itemid == 50827: #Blood ventilation rate
        for v in values:
            values[v] = tuple(None if i=='' else float(i)
                    for i in v.strip().split("/"))
            assert len(values[v]) == 2
        return values, ('float','float')

    if not is_float and 'D' in values:
        one_value = True
        for v in values:
            if v[0] == 'D' or v[0] == 'S' or v == 'MADE':
                values[v] = 1
            elif 'UNABLE' in v or v[:3] == 'ERR':
                values[v] = 0
                one_value = False
            else:
                values[v] = None
    elif not is_float and len(values)<= 20 and (
            'POS' in values or 'POSITIVE' in values or
            'NEG' in values or 'NEGATIVE' in values):
        for v in values:
            if 'POS' in v and 'ERR' not in v:
                values[v] = 1
            elif 'NEG' in v and 'ERR' not in v:
                values[v] = 0
            else:
                values[v] = None
    elif not is_float:
        num_ranges = 0
        for v in values:
            if range_rex.match(v) is not None:
                num_ranges += 1
        if num_ranges >= len(values) - 1:
            is_float = True

    if is_float:
        for v in values:
            if itemid == 51076:
                v = v.replace("FIVE", "5")
            values[v] = clean_v(v)
                    
    for v in values:
        if 'ERR' in v:
            values[v] = None
    if is_float:
        t = 'float'
    elif len(values) <= 2:
        t = 'bool'
    else:
        t = 'categorical'
    return values, t

def pretty_print_item(it, f):
    print(itemid, end=": {\n", file=f)
    for i in 'name', 'label':
        print(" "*4 + repr(i) + ': ' + repr(it[i]) + ',', file=f)
    print(" "*4 + '"type":', repr(it['type'])+',', '# n. floats =', end='', file=f)
    if float in cats['categories']:
        print('', cats['categories'][float], end=' ', file=f)
    print('/', cats['frequency'], file=f)
    print(" "*4 + '"values": {', end='', file=f)
    for k, v in it['values'].items():
        print("", file=f)
        print(" "*8 + repr(k) + ': ' + repr(v) + ',', end='', file=f)
    print("}},", file=f)

if __name__ == '__main__':
    with open('labevents_item_categories.pkl', 'rb') as f:
        labevents = pickle.load(f)

    with open('d_labitems.csv', 'r') as csvfile:
        f = iter(csv.reader(csvfile))
        next(f)
        item_names = dict(map(lambda t: (int(t[0]), t[1:]), f))

    litems = list(labevents.items())
    litems.sort(key=lambda k: -k[1]['frequency'])
    with open('GEN_labevents_clean.py', 'w') as f:
        print("events = {", file=f)
        for itemid, cats in litems:
            if itemid in excluded_itemid:
                continue

            it = {"name": item_names[itemid][0],
                  "label": item_names[itemid][1]}
            it["type"] = itemid in to_make_float or (float in cats['categories'] and
                              cats['categories'][float] > cats['frequency']/2)
            it["values"] = dict(filter(lambda i: i[0] is not float,
                                       cats['categories'].items()))

            it["values"], it["type"] = \
                    correct_values(it["values"], it["type"], itemid)
            pretty_print_item(it, f)
            
        print("}", file=f)
