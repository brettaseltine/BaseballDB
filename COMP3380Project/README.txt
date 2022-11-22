Compile with the command: java -cp mssql-jdbc-11.2.0.jre11.jar DBDriver.java'

When the program is run, it will initially create and populate the database. This takes about 3 minutes.

If you decide to run the program again, there is no need to re-create and re-populate the database so you can comment out the methods createDB() and populateDB() on lines 33 and 34 respectively.
Doing so allows you do immediately access the interface.