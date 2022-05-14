import happybase

connection = happybase.Connection('mipt-node02.atp-fivt.org')

connection.create_table(
    'pakhtyamov_email_happybase',
    families={
        'mail_body': dict(),
        'sender': dict(),
        'cc': dict(), # copy
        'bcc': dict() # hidden copy
    }
)
