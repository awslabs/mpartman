### [pgTAP tests](https://pgtap.org/)

---
#### Install pgTAP extension
Connect to your database as a superuser and run
```sql
create extension pgtap;
```

---
#### Run the tests
Connect to your database as a superuser using psql and run
```sql
\ir /path/to/Mpartman/tests/main_pgtap_script.sql
```
