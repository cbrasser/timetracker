# timetracker
Tiny timetracker written in plain c



## Usage

compile with gcc and start

Insert a record:
'insert [project name] [hours worked (float)]'

-> Inserts a record into a db-like structure and ads a timestamp

Get records to stdout:
'select'
-> just prints all records atm

'total [project name]'
-> prints total time worked on a project, or on all projects if no name provided

'average [project name]'
-> prints average time worked on a project, or on all projects if no name provided

## Attention

Database file is only written if the programm is closed with '.exit' and not interrupted via sigint or likewise.
