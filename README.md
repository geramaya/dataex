# Dataexporter

The application LocApp is a command line interface (CLI) application. LocApp recursively collects
all available * .properties files from a root directory and imports the path and value information into an h2 database.

###  List of commands:

        (q)uit:                                 Quit the program.
        command options mandatory:              Command parameters without brackets are mandatory
        command options optional:               Command parameters inside brackets are optional
        (h)elp:                                 Print this!


### Requirements:

- Java 8.x
- Apache Maven 3.0.5 or higher
- All properties files should be formatted as utf-8 without BOM (byte order mark)

###  Install and run:

- Clone the project
- Build the project with maven
- Start application with: java -jar target/locapp-x.x.x-XYZ-jar-with-dependencies.jar
