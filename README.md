# tap-youtrack
Currently not licensed.
lookatfirewall@gmail.com

# Using:
1. Download package
2. Install package 
```
pip install -e .
```

3. Set up decouple env config using template
```
cp .env_template .env
```
4. Run as package
```
tap-youtrack -c ''
```

5. if you want use some targets, for example target-postgres
```
pip install git+https://github.com/datamill-co/target-postgres.git
tap-youtrack -c '' | tap-postges --config config.json
```

Tested with Python 3.10, postgresql-14 and latest requirements.


# Backlog 

1. Improve custom fields parsing with respect to Youtrack field Type 
2. Introduce `only` projects filter 
3. Add entities:
   1. Tags 
   2. Users
   3. Boards 
4. Improve activities parsing, consider using Activities Page https://www.jetbrains.com/help/youtrack/devportal/resource-api-activitiesPage.html