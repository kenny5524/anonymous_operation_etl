
import numpy as np
event_root_codes_map = {
'14': 'PROTESTS',
'18': 'ASSAULT',
'19': 'FIGHT',
'20': 'USE UNCONVENTIONAL MASS VIOLENCE'
}


event_base_codes_map = {
'140': 'Engage in political dissent, not specified below',
'141': 'Demonstrate or rally, not specified below',
'142': 'Conduct hunger strike, not specified below',
'143': 'Conduct strike or boycott, not specified below',
'144': 'Obstruct passage, block, not specified below',
'145': 'Protest violently, riot, not specified below',
'180': 'Use unconventional violence, not specified below',
'181': 'Abduct, hijack, or take hostage',
'182': 'Physically assault, not specified below',
'183': 'Conduct suicide, car, or other non-military bombing, not specified below',
'184': 'Use as human shield',
'185': 'Attempt to assassinate',
'186': 'Assassinate',
'190': 'Use conventional military force, not specified below',
'191': 'Impose blockade, restrict movement',
'192': 'Occupy territory',
'193': 'Fight with small arms and light weapons',
'194': 'Fight with artillery and tanks',
'195': 'Employ aerial weapons, not specified below',
'196': 'Violate ceasefire',
'200': 'Use unconventional mass violence, not specified below',
'201': 'Engage in mass expulsion',
'202': 'Engage in mass killings',
'203': 'Engage in ethnic cleansing',
'204': 'Use weapons of mass destruction, not specified below'


}
event_codes_map = {
'140': 'Engage in political dissent, not specified below',
'141': 'Demonstrate or rally, not specified below',
'1411': 'Demonstrate for leadership change',
'1412': 'Demonstrate for policy change',
'1413': 'Demonstrate for rights',
'1414': 'Demonstrate for change in institutions, regime',
'142': 'Conduct hunger strike, not specified below',
'1421': 'Conduct hunger strike for leadership change',
'1422': 'Conduct hunger strike for policy change',
'1423': 'Conduct hunger strike for rights',
'1424': 'Conduct hunger strike for change in institutions, regime',
'143': 'Conduct strike or boycott, not specified below',
'1431': 'Conduct strike or boycott for leadership change',
'1432': 'Conduct strike or boycott for policy change',
'1433': 'Conduct strike or boycott for rights',
'1434': 'Conduct strike or boycott for change in institutions, regime',
'144': 'Obstruct passage, block, not specified below',
'1441': 'Obstruct passage to demand leadership change',
'1442': 'Obstruct passage to demand policy change',
'1443': 'Obstruct passage to demand rights',
'1444': 'Obstruct passage to demand change in institutions, regime',
'145': 'Protest violently, riot, not specified below',
'1451': 'Engage in violent protest for leadership change',
'1452': 'Engage in violent protest for policy change',
'1453': 'Engage in violent protest for rights',
'1454': 'Engage in violent protest for change in institutions, regime',
'180': 'Use unconventional violence, not specified below',
'181': 'Abduct, hijack, or take hostage',
'182': 'Physically assault, not specified below',
'1821': 'Sexually assault',
'1822': 'Torture',
'1823': 'Kill by physical assault',
'183': 'Conduct suicide, car, or other non-military bombing, not specified below',
'1831': 'Carry out suicide bombing',
'1832': 'Carry out vehicular bombing',
'1833': 'Carry out roadside bombing',
'1834': 'Carry out location bombing',
'184': 'Use as human shield',
'185': 'Attempt to assassinate',
'186': 'Assassinate',
'190': 'Use conventional military force, not specified below',
'191': 'Impose blockade, restrict movement',
'192': 'Occupy territory',
'193': 'Fight with small arms and light weapons',
'194': 'Fight with artillery and tanks',
'195': 'Employ aerial weapons, not specified below',
'1951': 'Employ precision-guided aerial munitions',
'1952': 'Employ remotely piloted aerial munitions',
'196': 'Violate ceasefire',
'200': 'Use unconventional mass violence, not specified below',
'201': 'Engage in mass expulsion',
'202': 'Engage in mass killings',
'203': 'Engage in ethnic cleansing',
'204': 'Use weapons of mass destruction, not specified below',
'2041': 'Use chemical, biological, or radiological weapons',
'2042': 'Detonate nuclear weapons'
}


map_fips_to_iso2_map = {
'AF': 'AF', 'AX': np.NaN, 'AL': 'AL', 'AG': 'DZ', 'AQ': 'AS', 'AN': 'AD', 'AO': 'AO', 'AV': 'AI', 'AY': 'AQ',
'AC': 'AG',
'AR': 'AR', 'AM': 'AM', 'AA': 'AW', 'AT': 'AU', 'AS': 'AU', 'AU': 'AT', 'AJ': 'AZ', 'BF': 'BS', 'BA': 'BH', 'FQ':
'UM',

'BG': 'BD', 'BB': 'BB', 'BS': 'RE', 'BO': 'BY', 'BE': 'BE', 'BH': 'BZ', 'BN': 'BJ', 'BD': 'BM', 'BT': 'BT', 'BL':
'BO',
'BK': 'BA', 'BC': 'BW', 'BV': 'BV', 'BR': 'BR', 'IO': 'IO', 'BX': 'BN', 'BU': 'BG', 'UV': 'BF', 'BM': 'MM', 'BY':
'BI',
'CB': 'KH', 'CM': 'CM', 'CA': 'CA', 'CV': 'CV', 'CJ': 'KY', 'CT': 'CF', 'CD': 'TD', 'CI': 'CL', 'CH': 'CN', 'KT':
'CX',
'IP': 'PF', 'CK': 'CC', 'CO': 'CO', 'CN': 'KM', 'CG': 'CD', 'CF': 'CG', 'CW': 'CK', 'CR': 'AU', 'CS': 'CR',
'IV': 'CI',
'HR': 'HR', 'CU': 'CU', 'UC': 'CW', 'CY': 'CY', 'EZ': 'CZ', 'DA': 'DK', 'DX': np.NaN, 'DJ': 'DJ', 'DO': 'DM',
'DR': 'DO',
'EC': 'EC', 'EG': 'EG', 'ES': 'SV', 'EK': 'GQ', 'ER': 'ER', 'EN': 'EE', 'ET': 'ET', 'PJ': np.NaN, 'EU': 'RE',
'FK': 'FK',
'FO': 'FO', 'FJ': 'FJ', 'FI': 'FI', 'FR': 'FR', 'FG': 'GF', 'FP': 'PF', 'FS': 'TF', 'GB': 'GA', 'GA': 'GM', 'GZ':
'PS',
'GG': 'GE', 'GM': 'DE', 'GH': 'GH', 'GI': 'GI', 'GO': 'RE', 'GR': 'GR', 'GL': 'GL', 'GJ': 'GD', 'GP': 'GP',
'GQ': 'GU',
'GT': 'GT', 'GK': 'GB', 'GV': 'GN', 'PU': 'GW', 'GY': 'GY', 'HA': 'HT', 'HM': 'HM', 'HO': 'HN', 'HK': 'HK',
'HQ': 'UM',
'HU': 'HU', 'IC': 'IS', 'IN': 'IN', 'ID': 'ID', 'IR': 'IR', 'IZ': 'IQ', 'EI': 'IE', 'IM': 'GB', 'IS': 'IL', 'IT': 'IT',
'JM': 'JM', 'JN': 'SJ', 'JA': 'JP', 'DQ': 'UM', 'JE': 'GB', 'JQ': 'UM', 'JO': 'JO', 'JU': 'RE', 'KZ': 'KZ', 'KE':
'KE',
'KQ': 'UM', 'KR': 'KI', 'KN': 'KP', 'KS': 'KR', 'KV': np.NaN, 'KU': 'KW', 'KG': 'KG', 'LA': 'LA', 'LG': 'LV',
'LE': 'LB',
'LT': 'LS', 'LI': 'LR', 'LY': 'LY', 'LS': 'LI', 'LH': 'LT', 'LU': 'LU', 'MC': 'MO', 'MK': 'MK', 'MA': 'MG', 'MI':
'MW',
'MY': 'MY', 'MV': 'MV', 'ML': 'ML', 'MT': 'MT', 'RM': 'MH', 'MB': 'MQ', 'MR': 'MR', 'MP': 'MU', 'MF': 'YT',
'MX': 'MX',
'FM': 'FM', 'MQ': 'UM', 'MD': 'MD', 'MN': 'MC', 'MG': 'MN', 'MJ': 'ME', 'MH': 'MS', 'MO': 'MA', 'MZ':
'MZ', 'WA': 'NA',
'NR': 'NR', 'BQ': 'UM', 'NP': 'NP', 'NL': 'NL', 'NC': 'NC', 'NZ': 'NZ', 'NU': 'NI', 'NG': 'NE', 'NI': 'NG', 'NE':
'NU',
'NF': 'NF', 'CQ': 'MP', 'NO': 'NO', 'MU': 'OM', 'PK': 'PK', 'PS': 'PW', 'LQ': 'UM', 'PM': 'PA', 'PP': 'PG',
'PF': np.NaN,
'PA': 'PY', 'PE': 'PE', 'RP': 'PH', 'PC': 'PN', 'PL': 'PL', 'PO': 'PT', 'RQ': 'PR', 'QA': 'QA', 'RE': 'RE', 'RO':
'RO',
'RS': 'RU', 'RW': 'RW', 'TB': 'BL', 'SH': 'SH', 'SC': 'KN', 'ST': 'LC', 'RN': 'MF', 'SB': 'PM', 'VC': 'VC',
'WS': 'WS',
'SM': 'SM', 'TP': 'ST', 'SA': 'SA', 'SG': 'SN', 'RI': 'RS', 'SE': 'SC', 'SL': 'SL', 'SN': 'SG', 'NN': 'SX', 'LO':
'SK',
'SI': 'SI', 'BP': 'SB', 'SO': 'SO', 'SF': 'ZA', 'SX': 'GS', 'OD': 'SS', 'SP': 'ES', 'PG': np.NaN, 'CE': 'LK',
'SU': 'SD',
'NS': 'SR', 'SV': 'SJ', 'WZ': 'SZ', 'SW': 'SE', 'SZ': 'CH', 'SY': 'SY', 'TW': 'TW', 'TI': 'TJ', 'TZ': 'TZ', 'TH':
'TH',
'TT': 'TL', 'TO': 'TG', 'TL': 'TK', 'TN': 'TO', 'TD': 'TT', 'TE': 'UM', 'TS': 'TN', 'TU': 'TR', 'TX': 'TM', 'TK':
'TC',

'TV': 'TV', 'UG': 'UG', 'UP': 'UA', 'AE': 'AE', 'UK': 'GB', 'US': 'US', 'UY': 'UY', 'UZ': 'UZ', 'NH': 'VU', 'VT':
'VA',
'VE': 'VE', 'VM': 'VN', 'VI': 'VG', 'VQ': 'VI', 'WQ': 'UM', 'WF': 'WF', 'WE': 'PS', 'WI': 'EH', 'YM': 'YE',
'ZA': 'ZM',
'ZI': 'ZW'}

