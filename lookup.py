import os
import csv
import re
import sys
from datetime import *
from sqlalchemy import create_engine
from sqlalchemy.sql import text

db_uri = os.environ['DB_URI']

name_re = re.compile(r'^(?P<last_name>[A-Z-\' ]+?), (?P<first_name>[A-Z-\']+?) (?:(?P<middle_name>[A-Z-\' ]+?)\.?)?(?: (?P<suffix>JR|SR|II|2ND|III|3RD)?)?$')

query = """
select
  :first_name,
  :last_name,
  address_1,
  address_2,
  city,
  state,
  zip_code
from
  odytraf_defendants
where
  name like :name
  and (sex = :sex or sex = :full_sex)
  and cast(
    substring(
      "DOB_str"
      from
        '%/#"[0-9]{4}#"' for '#'
    ) as INTEGER
  ) between :year_min
  and :year_max
union
select
  :first_name,
  :last_name,
  address_1,
  address_2,
  city,
  state,
  zip_code
from
  dscr_defendants
where
  name like :name
  and (sex = :sex or sex = :full_sex)
  and cast(
    substring(
      "DOB_str"
      from
        '%/#"[0-9]{4}#"' for '#'
    ) as INTEGER
  ) between :year_min
  and :year_max
union
select
  :first_name,
  :last_name,
  address_1,
  address_2,
  city,
  state,
  zip_code
from
  dsk8_defendants
where
  name like :name
  and (sex = :sex or sex = :full_sex)
  and cast(
    substring(
      "DOB_str"
      from
        '%/#"[0-9]{4}#"' for '#'
    ) as INTEGER
  ) between :year_min
  and :year_max;
"""

if __name__ == '__main__':
    persons = {}
    filename = sys.argv[1]
    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            fullname = row['Name']
            match = name_re.fullmatch(fullname)
            if not match:
                raise Exception('Failed to match on name: {}'.format(fullname))
            first_name = match.group('first_name')
            middle_name = match.group('middle_name')
            last_name = match.group('last_name')
            suffix = match.group('suffix')
            age = int(row['Age'])
            sex = row['Sex']
            filed_date = datetime.strptime(row['Date'],'%m/%d/%y').date()
            key = '{}-{}'.format(fullname,age)
            if key not in persons:
                persons[key] = {
                    'fullname': fullname,
                    'first_name': first_name,
                    'middle_name': middle_name,
                    'last_name': last_name,
                    'suffix': suffix,
                    'age': age,
                    'sex': sex,
                    'filed_dates': [filed_date]
                }
            else:
                if filed_date not in persons[key]['filed_dates']:
                    persons[key]['filed_dates'].append(filed_date)
    # print(persons)
    engine = create_engine(db_uri, echo=False)
    conn = engine.connect()
    stmt = text(query)
    with open('found.csv', 'w', newline='') as csvfile:
        fieldnames = [
            'first_name',
            'last_name',
            'address_1',
            'address_2',
            'city',
            'state',
            'zip_code'
        ]
        writer = csv.writer(csvfile)
        writer.writerow(fieldnames)
        for key, person in persons.items():
            # for filing_date in person['filed_dates']:
            params = {
                'first_name': person['first_name'],
                'last_name': person['last_name'],
                'name':'{}, {}%'.format(person['last_name'],person['first_name']),
                'sex':person['sex'],
                'full_sex':'Male' if person['sex'] == 'M' else 'Female',
                # 'filing_date':filing_date,
                'year_min':2015 - person['age'],
                'year_max':2018 - person['age']
            }
            ret = conn.execute(stmt,params)
            results = ret.fetchall()
            if results:
                for result in results:
                    print(result)
                    writer.writerow(list(result))
            # else:
                # print('No results for {}'.format(key))
    print('Done.')
