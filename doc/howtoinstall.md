## Package installation
You can install Mpartman in your PostgreSQL database that runs on-premises or in the AWS Cloud. You can install Mpartman as an extension or as a set of functions.  
  
---
### Install as an extension.
> **Note:** You can install the package as an extension into either the bare metal host or virtual machine.  

> **Prerequisite:** The PostgreSQL development package must be installed on the host.

1. Clone the git repository.
2. Change directory to mpartman.
3. Run `sudo make install`.
4. Connect to your database using psql as a superuser.
5. Run the following commands (supposing you'll use the schema mpartman):
```
create schema mpartman;
create extension mpartman schema mpartman;
```
  
---
### Install as a set of functions
> **Note:** In this way the package can be installed anywhere.

1. Clone the git repository.
2. Connect to your database using psql as a superuser.
3. Run the following command:
```
\ir /path/to/repository/mpartman/psql-install.sql
```
  
---
### Permissions
If you want to run the Mpartman functions not as a superuser only, you need to grant the privileges to the corresponding users:
```
select mpartman.f_grant_package_privileges('some_user_name');
```

