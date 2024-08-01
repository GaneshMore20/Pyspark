# Import necessary libraries
import pyodbc

# check driver
pyodbc.drivers() #drivers takes the query from notebook & go to database & execute it & gives results back in notebook

#connection details
server = 'server_name'
database = 'database_name'
UID = 'username'
PWD = 'password'

# Connect to the database # cnxn will hold whole connection string
cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)

#create cursor object
cursor = cnxn.cursor()

#on top the cursor object, there is execute function in which we can run sql query
cursor.execute("select * from table_name") #this will give output in object format

#to get proper data, we'll need to use for loop
for row in cursor:
    print(row) #this will print each row of the table

# in order to use insert query in execute function, we'll need to commit it.
cursor.execute("insert into table_name values (a, bc, c)")
cursor.commit()

#lastly make sure to close cursor object & connection object
cursor.close()
cnxn.close()