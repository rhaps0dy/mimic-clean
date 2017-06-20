####
# This file contains a dictionary to obtain clean values from labevents rows
####

import GEN_labevents_clean

__all__ = ['labevents_value_translation']

labevents_value_translation = {
# Must treat this one specially, split it into two measurements
#50827: {
#    'name': 'Ventilation Rate',
#    'label': 'Blood',
#    'one_value': False,
#    "is_float": False, # n. floats =/ 71995
#    "values": {}},
#50975: {
#    'name': 'Protein Electrophoresis',
#    'label': 'Blood',
#    'one_value': False,
#    "is_float": False, # n. floats =/ 3034
#    "values": {}, #Free text field
#    },
#51098: {
#    'name': 'Prot. Electrophoresis, Urine',
#    'label': 'Urine',
#    'one_value': False,
#    "type": False, # n. floats =/ 1778
#    "values": {} #Free text field},
#50948: {
#    'name': 'Immunofixation',
#    'label': 'Blood',
#    'one_value': False,
#    "is_float": False, # n. floats =/ 815
#    "values": {} #Free text field },
#51086: {
#    'name': 'Immunofixation, Urine',
#    'label': 'Urine',
#    'one_value': False,
#    "is_float": False, # n. floats =/ 791
#    "values": {}}, #Free text field
51492: {
    'name': 'Protein - Urine',
    'one_value': False,
    "type": 'float', # n. floats = 35965 / 106071
    "values": {
        'NEGATIVE': 0,
        'ERROR DISREGARD PREVIOUS RESULT OF NEG': None,
        'N': 0,
        'ERROR': None,
        'COMPUTER NETWORK FAILURE. TEST NOT RESULTED.': None,
        'NOT DONE': None,
        'TR': 10, # means "Traces"
        'UNABLE TO REPORT': None,
        '>300': 330,
        '>600': 660,
        'Neg': 0,
        'NEG': 0,
        ' ': None,
        'Tr': 1,}},
51514: {
    'name': 'Urobilinogen - Urine',
    'one_value': False,
    "type": 'float', # n. floats = 24992 / 101476
    "values": {
        'NEG': -1,
        'ERROR DISREGARD PREVIOUS RESULT OF NEG': None,
        '>12': 13.2,
        'ERROR': None,
        'NotDone': None,
        'COMPUTER NETWORK FAILURE. TEST NOT RESULTED.': None,
        'NOT DONE': None,
        '>8.0': 8.8,
        ' ': None,
        'UNABLE TO REPORT': None,
        'N': -1,
        '>8': 8.8,}},
51484: {
    'name': 'Ketone - Urine',
    'one_value': False,
    "type": 'float', # n. floats = 10600 / 101283
    "values": {
        'ERROR DISREGARD PREVIOUS RESULT OF NEG': 0,
        'T': 1,
        '>80': 88,
        'ERROR': None,
        'COMPUTER NETWORK FAILURE. TEST NOT RESULTED.': None,
        'NOT DONE': None,
        'TR': 1, # Traces
        'UNABLE TO REPORT': None,
        'N': 0,
        'Neg': 0,
        'Tr': 10,
        ' ': None,
        'NEG': 0,}},
51478: {
    'name': 'Glucose - Urine',
    'one_value': False,
    "type": 'bool', # n. floats = 11103 / 100939
    "values": {
        'Neg': 0,
        'NEG': 0,
        'ERROR DISREGARD PREVIOUS RESULT OF NEG': None,
        '>1000': 1,
        'ERROR': None,
        'N': 0,
        'COMPUTER NETWORK FAILURE. TEST NOT RESULTED.': None,
        'TR': 1,
        'NOT DONE': None,
        ' ': None,
        'UNABLE TO REPORT': None,}},
51508: {
    'name': 'Urine Color - Urine',
    'one_value': False,
    # Technically not float, but ordered
    "type": ('float', 'categorical'), # n. floats =/ 100036
    "values": {
        'None': (0, 0),
        'NONE': (0, 0),
        'Clear': (0, 0),
        'TELLOW': (1, 0),
        'YEL': (1, 0),
        'Y': (1, 0),
        'Yellow': (1, 0),
        'YELLOW': (1, 0),
        'YELL': (1, 0),
        'YELLO': (1, 0),
        'STR': (2, 0),
        'Straw': (2, 0),
        'S': (2, 0),
        'STRAW': (2, 0),
        'DKYELLOW': (2, 0),
        'LT AMB': (3, 0),
        'LtAmb': (3, 0),
        'DKAMBE': (3, 0),
        'LTAMB': (3, 0),
        'LT': (3, 0),
        'Lt': (3, 0),
        'A': (4, 0),
        'AMB': (4, 0),
        'AMBER': (4, 0),
        'AM': (4, 0),
        'Amber': (4, 0),
        'Dk': (5, 0),
        'DRKAMBER': (5, 0),
        'DK': (5, 0),
        'DKA': (5, 0),
        'DKAML': (5, 0),
        'DkAmb': (5, 0),
        'DKAMBER': (5, 0),
        'DKAMB': (5, 0),
        'DK AMB': (5, 0),
        'LTBROWN': (6, 0),
        'BROWN': (7, 0),
        'Brown': (7, 0),
        'B': (7, 0),
        'Red': (7, 0),
        'BROWN ': (7, 0),
        'DKBROWN': (8, 0),
        'PINK': (0, 1),
        'Pink': (0, 1),
        'P': (0, 1),
        'RED': (1, 1),
        'BLOODY': (1, 1),
        'R': (1, 1),
        'Green': (0, 2),
        'GREEN': (0, 2),
        'Orange': (0, 3),
        'ORANGE': (0, 3),
        'Black': (0, 4),
        'BLUE': (0, 5),
        'HAZY': (0, 6),
        'Other': (0, 7),
        'O': (0, 7),
        'OTHER': (0, 7),
        'ABN COLOR MAY AFFECT DIPSTICK': (None, None),
        'COMPUTER NETWORK FAILURE. TEST NOT RESULTED.': (None, None),
        'ERROR': (None, None),
        'VOID': (None, None),
        ' ': (None, None),
        'ERROR DISREGARD PREVIOUS RESULT OF YELLOW': (None, None),}},
51506: {
    'name': 'Urine Appearance - Urine',
    'one_value': False,
    'type': ('float', 'float'), # n. floats =/ 99497
    "values": {
        'Clear': (0, 0),
        'CLEAR': (0, 0),
        'C': (0, 0),
        'SLHAZY': (1, 0),
        'Sl': (1, 0),
        'SL': (1, 0),
        'S': (1, 0),
        'SlHazy': (1, 0),
        'HA': (2, 0),
        'H': (2, 0),
        'HAZY': (2, 0),
        'Hazy': (2, 0),
        'SlCldy': (3, 0),
        'SLCLOUDY': (3, 0),
        'SlCloudy': (3, 0),
        'Slcldy': (3, 0),
        'SLIGHTLY CLOUDY': (3, 0),
        'SLCLDY': (3, 0),
        'Cloudy': (4, 0),
        'CL': (4, 0),
        'CLOUD': (4, 0),
        'CLO': (4, 0),
        'CLOU': (4, 0),
        'CLOUDY': (4, 0),
        'CLDY': (4, 0),
        'TURBID': (5, 0),
        'Turbid': (5, 0),
        'PINK': (0, 1),
        'RED': (0, 2),
        'ERROR': None,
        ' ': None,
        'ERROR DISREGARD PREVIOUS RESULT OF HAZY': None,
        'COMPUTER NETWORK FAILURE. TEST NOT RESULTED.': None,}},
51466: {
    'name': 'Blood - Urine',
    'one_value': False,
    # Originally not float but converted
    "type": 'float', # n. floats =/ 99385
    "values": {
        'N': 0,
        'NEG': 0,
        'NEGATIVE': 0,
        'Neg': 0,
        'TRACE': 1,
        'TR': 1,
        'SM': 2,
        'SMALL': 2,
        'Sm': 2,
        'MOD': 3,
        'Mod': 3,
        'LGE': 4,
        'LG': 4,
        'Lg': 4,
        'ERROR DISREGARD PREVIOUS RESULT OF NEG': None,
        'ERROR': None,
        'Jul-17-03': None,
        'COMPUTER NETWORK FAILURE. TEST NOT RESULTED.': None,
        'NOT DONE': None,
        'UNABLE TO REPORT': None,
        'Sep-19-02': None,
        ' ': None,}},
51464: {
    'name': 'Bilirubin - Urine',
    'one_value': False,
    # Originally not float
    "type": 'float', # n. floats =/ 99339
    "values": {
        'LG': 3,
        'M': 3,
        'Neg': 0,
        'MOD': 2,
        'ERROR': None,
        'COMPUTER NETWORK FAILURE. TEST NOT RESULTED.': None,
        'SM': 1,
        'BEH': None,
        'NOT DONE': None,
        'Specimen': None,
        'UNABLE TO REPORT': None,
        'N': 0,
        'ERROR DISREGARD PREVIOUS RESULT OF NEG': None,
        ' ': None,
        'NEG': 0,}},
51487: {
    'name': 'Nitrite - Urine',
    'one_value': False,
    "type": 'bool', # n. floats =/ 99334
    "values": {
        'NEGATIVE': 0,
        'Neg': 0,
        'ERROR': None,
        'UNABLE TO REPORT': None,
        'COMPUTER NETWORK FAILURE. TEST NOT RESULTED.': None,
        'NOT DONE': None,
        ' ': None,
        'P': 1,
        'ERROR DISREGARD PREVIOUS RESULT OF NEG': None,
        'POS': 1,
        'N': 0,
        'NEG': 0,}},
51486: {
    'name': 'Leukocytes - Urine',
    'one_value': False,
    # Originally not float
    "type": 'float', # n. floats =/ 97288
    "values": {
        'LG': 4,
        'SMALL': 2,
        'MOD': 3,
        'ERROR': None,
        'ERROR DISREGARD PRVIOUS RESULT OF NEG': None,
        'SM': 2,
        'TR': 1,
        'NOT DONE': None,
        'UNABLE TO REPORT': None,
        'TRACE': 1,
        'L': 4,
        'N': 0,
        'COMPUTER NETWORK FAILURE. TEST NOT RESULTED.': None,
        ' ': None,
        'NEG': 0,}},
51519: {
    'name': 'Yeast - Urine',
    'one_value': False,
    "type": ('float', 'bool'), # n. floats = 56 / 85253
    "values": {
        'NONE': (0, 0),
        'N': (0, 0),
        'RARE': (1, 0),
        'FEW HYPHAE YEAST': (2, 0),
        'F': (2, 0),
        'FEW': (2, 0),
        'MOD': (3, 0),
        'MANY BUDDING YEAST': (4, 0),
        'MANY': (4, 0),
        'LOADED': (5, 0),
        'OCC': (0, 1),
        'NOTDONE': None,
        'ERROR': None,
        ' ': None,
        'UNABLE TO REPORT': None,
        'UNABLE TO DETERMINE': None,
        'VOID': None,}},
51463: {
    'name': 'Bacteria - Urine',
    'one_value': False,
    "type": ('float', 'bool'), # n. floats = 1038 / 73209
    "values": {
        'NEG': (0, 0),
        'NONE': (0, 0),
        'RARE': (1, 0),
        'R': (1, 0),
        'FEW': (2, 0),
        'F': (2, 0),
        ' FEW': (2, 0),
        ' F': (2, 0),
        "F'": (2, 0),
        'SOME': (3, 0),
        'MO': (4, 0),
        'MODERATE': (4, 0),
        'MOD-': (4, 0),
        'MOD': (4, 0),
        'MANY': (5, 0),
        'M': (5, 0),
        ' M': (5, 0),
        'LRG': (6, 0),
        'O9': (0, 1),
        '0CC': (0, 1),
        'OCC': (0, 1),
        'O': (0, 1),
        '0': (0, 1),
        'UNABLE TO QUANTITATE': None,
        'VOID': None,
        '7I': None,
        'ERROR': None,
        'UNABLE TO REPORT DUE TO CLOUDY SPECIMEN': None,
        'UNABLE TO DETERMINE': None,
        ' ': None,
        'NOTDONE': None,}},
51266: {
    'name': 'Platelet Smear - Blood',
    'one_value': False,
    'type': ('float', 'bool'), # n. floats =/ 38992
    "values": {
        'RARE': (0, 0),
        'VERY LOW': (1, 0),
        'LOW': (2, 0),
        'NORMAL': (3, 0),
        'HIGH': (4, 0),
        'VERY HIGH': (5, 0),
        'UNABLE TO ESTIMATE DUE TO PLATELET CLUMPS': (0, 1), }},
51512: {
    'name': 'Urine Mucous - Urine',
    'one_value': False,
    "type": ('float', 'bool'), # n. floats =/ 14187
    "values": {
        'NONE': (0, 0),
        'RARE': (1, 0),
        'FEW]': (2, 0),
        'FEW': (2, 0),
        'MO': (3, 0),
        'MOD': (3, 0),
        'MANY': (4, 0),
        'OCC': (0, 1),
        ' ': None,
        }},
51523: {
    'name': 'GR HOLD - URINE',
    'one_value': False,
    "type": 'bool', # n. floats =/ 11848
    "values": {
        'DONE': 1,
        'GOLD': 0,
        'HOLD': 0,}},
50880: {
    'name': 'Benzodiazepine Screen - Blood',
    'one_value': False,
    "type": 'bool', # n. floats =/ 11823
    "values": {
        'NEGATIVE': 0,
        'POS': 1,
        'NEGATIVE - Level less than 10 miu/ml': 0,
        'ERROR': None,
        'NEG': 0,}},
51462: {
    'name': 'Amorphous Crystals - Urine',
    'one_value': False,
    "type": ('float', 'bool'), # n. floats =/ 6817
    "values": {
        'NONE': (0, 0),
        'RARE': (1, 0),
        'FEW': (2, 0),
        'MOD': (3, 0),
        'MODERATE': (3, 0),
        'M': (4, 0),
        'MANY': (4, 0),
        'LOADED': (5, 0),
        'OCC': (6, 1),
        ' ': None,
        'ERROR': None,}},
51518: {
    'name': 'WBC Clumps - Urine',
    'one_value': False,
    "type": ('float', 'bool'), # n. floats =/ 2107
    "values": {
        'NONE': (0, 0),
        'RARE': (1, 0),
        'FEW': (2, 0),
        'F': (2, 0),
        'MOD': (3, 0),
        'MANY': (4, 0),
        'OCC': (0, 1),
        'O': (0, 1), }},
50857: {
    'name': 'Acetone - Blood',
    'one_value': False,
    "type": ('float', 'bool'), # n. floats =/ 1408
    "values": {
        'NEGATIVE': (0, 0),
        'NEG': (0, 0),
        'ABSENT': (0, 0),
        'TRACE': (1, 0),
        'SMALL': (2, 0),
        'MODERATE': (3, 0),
        'MEDIUM': (3, 0),
        'SMALL POSITIVE': (4, 0),
        'POS': (5, 0),
        'POSITIVE': (5, 0),
        'POS. LARGE': (5, 0),
        'LARGE': (6, 0),
        'ACETONE NOT DONE': (0, 1),
        'TEST CANCELLED': (0, 1),
        'NOT RUN DUE TO HEMOLYSIS': (0, 1),
        'UNABLE TO PERFORM TEST DUE TO HEMOLYSIS': (0, 1),
        'ACETONE NOT DONE DUE TO THE INTERFERENCE OF HEMOLYSIS': (0, 1),
        'UNABLE TO PERFORM DUE TO HEMOLYSIS': (0, 1),
        'ACETONE NOT DONE, DUE TO PRESENCE OF HEMOLYSIS': (0, 1),
        'CAN NOT BE DONE DUE TO INTERFERENCE OF HEMOLYSIS': (0, 1),
        'NOT DONE DUE TO INTERFERENCE OF HEMOLYSIS': (0, 1),
        'TEST NOT PERFOMED DUE TO HEMOLYSIS': (0, 1),
        'TEST NOT PERFORMED DUE TO HEMOLYSIS': (0, 1),
        'ACETONE NOT DONE DUE TO INTERFERENCE OF HEMOLYSIS': (0, 1),
        'TEST CANCELLED BY LAB DUE TO HEMOLYSIS': (0, 1),
        'NOT DONE DUE TO THE INTERFERENCE OF HEMOLYSIS': (0, 1),
        'SAMPLE IS TOO OLD': None,
        'NOT DONE': None,}},
51469: {
    'name': 'Calcium Oxalate Crystals - Urine',
    'one_value': False,
    "type": ('float', 'bool'), # n. floats =/ 1404
    "values": {
        'NONE': (0, 0),
        'RARE': (1, 0),
        'FEW': (2, 0),
        'MOD': (3, 0),
        'MANY': (4, 0),
        'OCC': (0, 1),}},
51513: {
    'name': 'Urine Specimen Type - Urine',
    'one_value': False,
    "type": 'categorical', # n. floats =/ 1057
    "values": {
        'UNK': 0,
        '?': 0,
        'CLEAN': 1,
        '2152D': 2,
        '1674M': 3,
        'CATH': 4,
        '175M': 5,
        '517L': 6,
        '383E': 7,
        'RANDOM': 8,
        'Random': 8,
        '720M': 9,
        '11104F': 10,
        '84M': 11,
        '858M': 12,
        '2364L': 13,
        '493C': 14,
        '82G': 15,
        'A.AB': 16,
        '845M': 17,
        ' ': None,
        'VOID': None,}},
51505: {
    'name': 'Uric Acid Crystals - Urine',
    'one_value': False,
    "type": ('float', 'bool'), # n. floats =/ 1056
    "values": {
        'NONE': (0, 0),
        'RARE': (1, 0),
        'FEW': (2, 0),
        'MOD': (3, 0),
        'MANY': (4, 0),
        ',OD': (0, 1),
        'OCC': (0, 1),}},
}

for k, v in GEN_labevents_clean.events.items():
    assert k not in labevents_value_translation
    labevents_value_translation[k] = v
