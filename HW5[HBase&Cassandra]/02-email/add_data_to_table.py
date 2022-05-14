import happybase

connection = happybase.Connection('mipt-node01.atp-fivt.org')

table = connection.table('pakhtyamov_email_happybase')

#table.put(b'a@b.com#inbox#20201201', {
#    b'mail_body:text':b'Hello world 2',
#    b'sender:email':b'c@d.com',
#    b'cc:b@b.com':b'1'
#})


#table.put(b'a@b.com#sent#20201129', {
#    b'mail_body:text':b'Hello world 2',
#    b'sender:email':b'a@b.com',
#    b'cc:c@d.com':b'1'
#})


table.put(b'a@e.com#inbox#20201129', {
    b'mail_body:text':b'Hello world 3',
    b'sender:email':b'c@d.com',
    b'cc:e@f.com':b'1'
})

