import csv

with open('chartevents.csv') as csvfile:
    f = csv.reader(csvfile)
    next(f)
    total_missing = total = 0
    total_rows = total_r_m = 0
    prev_hour = None
    for row in f:
        total_rows += 1
        total += len(row)-3
        total_missing += row.count('')
        if prev_hour is None:
            prev_hour = int(row[2])
        else:
            hour = int(row[2])
            if hour != prev_hour+1 and hour > prev_hour:
                total_r_m += hour-prev_hour-1
                total_rows += hour-prev_hour-1
            prev_hour = hour
    print(total_missing, total, total_r_m, total_rows)
