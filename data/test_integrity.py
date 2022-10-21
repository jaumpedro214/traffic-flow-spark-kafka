import pandas as pd

files = [
    f'./AGOSTO_2022/20220801/20220801_{hour:02d}.json'
    for hour 
    in range(0, 23+1)
]

# Count the number of lines in each file
# and sum the total
total = 0
for file in files:
    df = pd.read_json(file, orient='records')
    print(f'{file}: {len(df)}')
    total += len(df)

print(total)