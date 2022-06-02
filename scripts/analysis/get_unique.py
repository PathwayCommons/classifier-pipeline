import csv

with open('ml.txt', 'r') as f_ml:
    ml = set()
    ml_contents = csv.reader(f_ml)
    for row in ml_contents:
        ml.add(row[0])

with open('invited.txt', 'r') as f_invited:
    invited = set()
    invited_contents = csv.reader(f_invited)
    for row in invited_contents:
        invited.add(row[0])

ml_unique = ml - invited

with open('ml_unique.txt', 'w') as f_ml_unique:
    lines = list(ml_unique)
    f_ml_unique.write('\n'.join(lines))
