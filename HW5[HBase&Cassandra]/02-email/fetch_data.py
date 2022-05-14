import happybase

connection = happybase.Connection('mipt-node01.atp-fivt.org')
table = connection.table('pakhtyamov_email_happybase')

print('CASE #1')
for user, data in table.scan():
    print(user, data)

print('CASE #2')
for user, data in table.scan(row_prefix=b'a@b.com#sent#2020', columns=[b'cc:c@e.com']):
    print(user, data)


print('CASE #3')
for user, data in table.scan(row_prefix=b'a@b.com#sent#2020'):
    print(user, data)


print('CASE #4')
for user, data in table.scan(row_prefix=b'a@b.com'):
    print(user, data)

# .com -> gmail, 
# EMAIL:
# com.b@a
